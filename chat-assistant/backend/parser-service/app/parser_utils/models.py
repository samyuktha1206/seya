from dataclasses import dataclass, field
from typing import Dict, Any
import hashlib

@dataclass
class Chunk:
    chunk_idx: int
    text: str
    text_hash: str = field(default_factory=str)
    token_counts: Dict[str, int] = field(default_factory=dict) #model_name -> token_count
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.text_hash:
            self.text_hash = sha256_hex(self.text)
    
    def set_token_count(self, model: str, count: int):
        self.token_counts[model] = count

def sha256_hex(s: str):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()