"""Prepare a temporary v2 fixture or compare its closed roots to their recorded digests.

    python3 tools/fjall-v2/fixture.py prepare /tmp/new-fixture
    python3 tools/fjall-v2/fixture.py check /tmp/new-fixture

After reset, pass both quarantine paths to the same original reader:

    cd /tmp/new-fixture
    ./reader read identities.json /path/to/quarantined-managed /path/to/quarantined-state
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


BASELINE = "62867fe7c8c59f11a67ab1db37ac2241ac1c6766"
TOOLS = Path(__file__).resolve().parent
ROOTS = ("managed", "state")


def build_reader(destination):
    with tempfile.TemporaryDirectory(prefix="mmdb-v2-build-") as directory:
        build = Path(directory)
        archive = build / "baseline.tar"
        subprocess.run(
            ["git", "archive", "--format=tar", "-o", archive, BASELINE],
            cwd=TOOLS.parents[1], check=True,
        )
        subprocess.run(["tar", "-xf", archive, "-C", build], check=True)
        shutil.copyfile(TOOLS / "fixture.rs", build / "crates/mmdb/examples/fjall_v2_fixture.rs")
        subprocess.run(
            ["rustup", "run", "1.97.1", "cargo", "build", "--locked", "-p", "mmdb",
             "--example", "fjall_v2_fixture", "--target-dir", build / "target"],
            cwd=build, check=True,
        )
        shutil.copy2(build / "target/debug/examples/fjall_v2_fixture", destination)


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inventory(directory):
    """Include empty directories and hash file contents independently of mtimes."""
    entries = {}

    def visit(path):
        name = str(path.relative_to(directory))
        if path.is_symlink():
            raise ValueError(f"unexpected symlink at {path}")
        if path.is_dir():
            entries[name] = {"directory": True}
            for child in sorted(path.iterdir()):
                visit(child)
        elif path.is_file():
            entries[name] = {"sha256": file_digest(path)}
        else:
            raise ValueError(f"missing or unsupported fixture entry at {path}")

    for root in ROOTS:
        visit(directory / root)
    return entries


def prepare(directory):
    directory.mkdir()
    reader = directory / "reader"
    build_reader(reader)
    paths = [directory / "identities.json", *(directory / root for root in ROOTS)]
    subprocess.run([reader, "write", *paths], check=True)
    for root in ROOTS:
        with (directory / root / "version").open("rb") as marker:
            if marker.read(4) != b"FJL\x02":
                raise ValueError(f"{root} was not written by a v2 engine")
    subprocess.run([reader, "read", *paths], check=True)
    (directory / "inventory.json").write_text(json.dumps(inventory(directory), indent=2) + "\n")


def check(directory):
    recorded = json.loads((directory / "inventory.json").read_text())
    current = inventory(directory)
    changed = sorted(k for k in recorded.keys() | current.keys() if recorded.get(k) != current.get(k))
    if changed:
        raise SystemExit("v2 fixture changed:\n" + "\n".join(changed))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("prepare", "check"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    {"prepare": prepare, "check": check}[args.action](args.directory.resolve())
