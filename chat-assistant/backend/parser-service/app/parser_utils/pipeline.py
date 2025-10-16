from typing import Optional, Dict, List, Tuple
from .models import Chunk
from .tokenizers import count_tokens, count_tokens_batch
from .tokenizers import create_tokenizer_registry
from .chunking import _split_long_paragraph, chunk_text_by_chars, chunk_text_to_objects


def enrich_chunks_with_token_counts(
    chunks: List[Chunk],
    models: List[str],
    registry: Dict[str, Dict[str, str]],
    batch: bool = True
) -> None:
    texts = [c.text for c in chunks]
    for model in models:
        if batch:
            counts = count_tokens_batch(texts, model, registry)
            for idx, cnt in enumerate(counts):
                chunks[idx].set_token_counts(model, cnt)
        else:
            for idx, t in enumerate(texts):
                count = count_tokens(t, model, registry)
                chunks[idx].set_token_count(model, count)