"""CP21S-A.2: verify and compress the durable complete timeline after the run.

The execution worker finishes and fsyncs the original JSONL first. The normal
analyzer validates the schema and row count. This archivist then independently
checks SHA256, row count and gzip integrity BEFORE deleting the original.
It performs streaming I/O and has no role in the economic hot path.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path


def _scan(stream):
    sha = hashlib.sha256()
    events = 0
    last_byte = None
    total_bytes = 0
    while chunk := stream.read(1024 * 1024):
        sha.update(chunk)
        total_bytes += len(chunk)
        events += chunk.count(b"\n")
        last_byte = chunk[-1]
    if last_byte != 10:
        raise ValueError("timeline is not newline terminated")
    return sha.hexdigest(), events, total_bytes


def archive(path: Path, expected_events: int, delete_raw=True):
    if expected_events < 0:
        raise ValueError("expected_events must be nonnegative")
    path = Path(path)
    packed = path.with_name(path.name + ".gz")
    temporary = packed.with_name(packed.name + ".tmp")
    if not path.is_file():
        raise FileNotFoundError(path)
    if packed.exists() or temporary.exists():
        raise FileExistsError("archive destination exists; refusing overwrite")
    try:
        with path.open("rb") as source:
            digest, count, raw_size = _scan(source)
        if count != expected_events:
            raise ValueError(f"journal row count mismatch: {count} != {expected_events}")
        with path.open("rb") as source, temporary.open("wb") as output:
            with gzip.GzipFile(fileobj=output, mode="wb", filename="",
                               compresslevel=1, mtime=0) as compressed:
                while chunk := source.read(1024 * 1024):
                    compressed.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        with gzip.open(temporary, "rb") as verified:
            compressed_digest, compressed_count, decompressed_bytes = _scan(verified)
        if (digest, count, raw_size) != (
                compressed_digest, compressed_count, decompressed_bytes):
            raise ValueError("gzip round-trip integrity mismatch")
        temporary.replace(packed)
        result = {
            "schema": 1,
            "source_file": path.name,
            "archive_file": packed.name,
            "format": "gzip-jsonl-schema1",
            "events": count,
            "sha256_uncompressed": digest,
            "uncompressed_bytes": raw_size,
            "compressed_bytes": packed.stat().st_size,
            "integrity_verified": True,
            "raw_deleted": bool(delete_raw),
        }
        manifest = path.with_name("economic_timeline_archive.json")
        # Preserve original until the final archive and metadata are durable.
        manifest.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if delete_raw:
            path.unlink()
        return result
    finally:
        if temporary.exists():
            temporary.unlink()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeline", type=Path, required=True)
    parser.add_argument("--expected-events", type=int, required=True)
    parser.add_argument("--keep-raw", action="store_true")
    args = parser.parse_args()
    result = archive(args.timeline, args.expected_events, not args.keep_raw)
    print(f"CP21S-A.2 timeline archived: rows={result['events']} "
          f"raw={result['uncompressed_bytes']} packed={result['compressed_bytes']} "
          f"file={result['archive_file']}")


if __name__ == "__main__":
    main()
