//! Read-only preflight for directories that may contain a storage engine.
use std::{
    ffi::OsStr,
    fs,
    io::{self, Read},
    path::Path,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EngineRoot {
    New,
    Existing,
}

/// Check the on-disk version before opening any engine in a compound store.
/// The owner lists the entries that can legitimately precede engine creation.
pub fn inspect(root: &Path, initial_entries: &[&str]) -> io::Result<EngineRoot> {
    inspect_with(root, |name| {
        initial_entries.iter().any(|allowed| name == *allowed)
    })
}

/// An established nested store can share its parent with application files.
/// Fjall 2 and 3 engine files still require a version marker in that parent.
pub fn inspect_enclosing(root: &Path) -> io::Result<()> {
    inspect_with(root, |name| {
        !matches!(
            name.to_str(),
            Some("partitions" | "journals" | "keyspaces" | "lock")
        ) && !Path::new(name)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jnl"))
    })?;
    Ok(())
}

fn inspect_with(
    root: &Path,
    allowed_without_marker: impl Fn(&OsStr) -> bool,
) -> io::Result<EngineRoot> {
    let marker = root.join("version");
    let contextual = |e: io::Error| io::Error::new(e.kind(), format!("{}: {e}", root.display()));
    let invalid = |reason: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: {reason}. Keep this directory and create a new empty store",
                root.display()
            ),
        )
    };
    match fs::symlink_metadata(&marker) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(invalid("invalid engine version marker"));
            }
            let mut bytes = [0u8; 4];
            fs::File::open(marker)
                .and_then(|mut file| file.read_exact(&mut bytes))
                .map_err(contextual)?;
            match &bytes {
                b"FJL\x03" => Ok(EngineRoot::Existing),
                b"FJL\x02" => Err(invalid("Fjall 2 is incompatible with Fjall 3")),
                _ => Err(invalid("unknown engine version")),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let entries = match fs::read_dir(root) {
                Ok(entries) => entries,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(EngineRoot::New),
                Err(e) => return Err(contextual(e)),
            };
            for entry in entries {
                let name = entry.map_err(contextual)?.file_name();
                if !allowed_without_marker(&name) {
                    return Err(invalid("nonempty directory is missing its engine version"));
                }
            }
            Ok(EngineRoot::New)
        }
        Err(e) => Err(contextual(e)),
    }
}
