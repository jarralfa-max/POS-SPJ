import gzip
import json

def compress_payload(events: list) -> bytes:
    data = json.dumps(events, separators=(",", ":")).encode("utf-8")
    return gzip.compress(data)

def decompress_payload(blob: bytes) -> list:
    data = gzip.decompress(blob)
    return json.loads(data.decode("utf-8"))