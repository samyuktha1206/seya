import re
from typing import List, Iterable
from .models import Chunk
from config import settings
from .tokenizer_registry import compute_max_chars, create_tokenizer_registry

registry = create_tokenizer_registry()
max_chars = compute_max_chars("gpt-40", "text-embedding-3-small", registry)
def _validate_params(max_chars: int, overlap: int):
    if max_chars <= 0:
        raise ValueError("max_chars must be > 0")
    if overlap < 0:
        raise ValueError("overlap must be >= 0")
    if overlap >= max_chars:
        raise ValueError("overlap must be smaller than max_chars")
    
def _split_long_paragraph(paragraph: str, max_chars: int, overlap: int) -> Iterable[str]:
    start = 0
    length = len(paragraph)

    while start < length:
        end = min(length, start + max_chars)
        window = paragraph[start:end]
        split_at = window.rfind(" ")

        if split_at <= 0:
            split_at = max_chars

        chunk = paragraph[start:start + split_at].rstrip()
        if not chunk:
            chunk = paragraph[start:max_chars]
        
        yield chunk
        next_start = start + split_at - overlap
        if next_start <= start:
            next_start = start + max(1, split_at//2)
        start = next_start

def chunk_text_by_chars(
        text: str,
        max_chars: int = max_chars,
        overlap: int = getattr(settings, "CHUNK_OVERLAP", 200)
) -> List[str]:
    _validate_params(max_chars, overlap)
    if not text:
        return []
    
    paragraphs = [p.strip() for p in re.split(r"r?\n", text) if p.strip()]
    chunks: List[str] = []
    current = ""

    for p in paragraphs:
        candidate = p if not current else f"{current}\n{p}"
        if len(candidate) <= max_chars:
            current = candidate
            continue
        
        if current:
            chunks.append(current)
            current = ""

        if len(p) <= max_chars:
            current = p
            continue
        
        for piece in _split_long_paragraph(p, max_chars, overlap):
            chunks.append(piece)
        current =""

    if current:
        chunks.append(current)
    
    return chunks

def chunk_text_to_objects(
        text: str,
        max_chars: int = max_chars,
        overlap: int = getattr(settings, "CHUNK_OVERLAP", 200)
) -> List[Chunk]:
    raw = chunk_text_by_chars(text, max_chars, overlap)
    return [Chunk(chunk_idx=i, text =c) for i, c in enumerate(raw)]
        
    