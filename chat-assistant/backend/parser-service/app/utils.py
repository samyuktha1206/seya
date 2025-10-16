import hashlib
import re
from typing import Dict, Any, List, Iterable
from dataclasses import dataclass
from config import settings

def sha256_hex(s: str) -> str:
  """ Return hex sha256 of the given string (utf-8)
  """
  return hashlib.sha256(s.encode("utf-8")).hexdigest()

@dataclass
class TextChunk:
    chunk_idx: int
    text: str

    @property
    def text_hash(self) -> str:
       return sha256_hex(self.text)
    
def _validate_params(max_chars: int, overlap: int) -> None:
    if max_chars <= 0:
       raise ValueError("max_chars must be > 0")
    if overlap < 0:
       raise ValueError("overlap must be >= 0")
    if overlap >= max_chars:
       raise ValueError("overlap must be < max_chars")
    
def _split_long_paragraph(paragraph: str, max_chars: int, overlap: int) -> Iterable[str]:
    """
    Splits a very long paragraph into small chunks no larger than max_chars.
    Prefers to split on white space, falls back to hard splits if no white space found.
    Ensures next start index moves forward by at least 1 char to avoid infinite loops.
    """

    start = 0
    length = len(paragraph)
    while start < length:
        #proposed window
        end = min(start + max_chars, length)
        window = paragraph[start:end]

        #try to find last whitespace in window for nicer boundries
        split_at = window.rfind(" ")
        if split_at == -1 or end == length:
            split_at = len(window)  #no whitespace found, hard split
        chunk = window[:split_at].strip()
        if not chunk:
            chunk = window[:start + max_chars]
        yield chunk

        #advance start index with overlap
        next_start = start + split_at - overlap
        if next_start <= start:
            #if overlap too large relative to split_at, move forward at least by chunk length minus 1
            next_start = start + max(1, split_at//2)
        start = next_start

def chunk_text(
        text: str,
        max_chars: int = getattr(settings, "CHUNK_MAX_CHARS", 2000),
        max_overlap: int = getattr(settings,"CHUNK_OVERLAP", 200)
) -> List[str]:
    """
    conservative chunking by character lenth with overlap; preserves paragraph boundries when possible
    Behavior:
    - Splits input into paragraphs by newline.
    - Tries to keep paragraphs intact if they fit within max_chars.
    - For paragraphs longer than max_chars, prefers to split on the last whitespace within the window,
      otherwise hard-slices the text.
    - Adds overlap between successive chunks of the same paragraph.
    - Newline characters count toward the max_chars budget.
    """
    _validate_params(max_chars, max_overlap)

    if not text:
        return []
    
    # Normalize newlines and split into non-empty paragraphs
    paragraphs = 