use std::fs;
use std::path::Path;

fn check_source(path: &Path) {
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            check_source(&entry.unwrap().path());
        }
    } else {
        let source = fs::read_to_string(path).unwrap();
        assert!(
            !source.contains("fjall"),
            "{} names the storage engine",
            path.display()
        );
    }
}

#[test]
fn store_modules_do_not_name_fjall() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for path in [
        "records",
        "context",
        "session",
        "store.rs",
        "native_memory.rs",
        "entity.rs",
        "extraction.rs",
    ] {
        check_source(&src.join(path));
    }
}
