import hashlib
import json
from typing import Dict

def sha256_hexdigest(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode('utf-8'))
    return h.hexdigest()

def chunk_message_to_items(msg: Dict):
    """
    Expect parser message like:
    {
      "correlationId": "abc",
      "url": "https://example.com",
      "chunks": [
        {"chunk_id":"c-1","text":"...","checksum":"sha256:...","pos":0},
        ...
      ],
      "parsed_at": "2025-10-15T10:00:00Z"
    }
    """
    items = []
    url = msg.get("url")
    correlationId = msg.get("correlationId")
    for c in msg.get("chunks", []):
        item = {
            "chunk_id": c["chunk_id"],
            "text": c["text"],
            "checksum": c.get("checksum") or sha256_hexdigest(c["text"]),
            "url": url,
            "pos": c.get("pos"),
            "correlationId": correlationId,
            "parsed_at": msg.get("parsed_at"),
        }
        items.append(item)
    return items
