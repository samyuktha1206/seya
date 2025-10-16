from typing import Dict, List, Any, Optional
import logging
from functools import lru_cache
from .tokenizer_registry import create_tokenizer_registry

logger = logging.getLogger(__name__)

#optional backends
_HAS_TIKTOKEN = False
_HAS_HF = False

try:
    import tiktoken
    _HAS_TIKTOKEN = True
except Exception:
    logger.debug("tiktoken not installed or failed to import")

try:
    from transformers import AutoTokenizer
    _HAS_HF = True
except Exception:
    logger.debug("transformers not installed or failed to import")
    _HF_TOKENIZERS = {}

#Default registry (logical model name -> backend & name)
# DEFAULT_MODEL_TOKENIZER_REGISTRY: Dict[str, Dict[str, str]] ={
#     "gpt-4o": {"backend": "tiktoken", "name" : "gpt-4o"},
#     "gpt-4-32k": {"backend": "tiktoken", "name": "gpt-4-32k"},
#     "text-embedding-3-small": {"backend": "tiktoken", "name": "text-embedding-3-small"},
#     "all-MiniLM-L6-v2": {"backend": "hf", "name":"all-MiniLM-L6-v2"}
# }

def _char_heuristic_count(text: str, avg_chars_per_token: float = 4.0) -> int:
    return max(1, int(len(text)/avg_chars_per_token))

@lru_cache(maxsize = 64)
def _get_hf_tokenizer(name: str):
    if not _HAS_HF:
        raise RuntimeError("transformers not available")
    tok = AutoTokenizer.from_pretrained(name, use_fast=True)
    return tok

registry = create_tokenizer_registry()

def count_tokens(text: str, model: str, registry: Optional[Dict[str, Dict[str, str]]] = None) -> int:
    if not text:
        return 0
    registry = registry #or DEFAULT_MODEL_TOKENIZER_REGISTRY
    entry = registry.get(model, {})
    backend = entry.get("backend")

    if backend == "tiktoken" and _HAS_TIKTOKEN:
        try:
            enc = tiktoken.encoding_for_model(entry["name"])
            return len(enc.encode(text))
        except Exception:
            logger.debug("tiktoken encoding failed for %s", model, exc_info=True)

    if backend == "hf" and _HAS_HF:
        try:
            tok = _get_hf_tokenizer(entry["name"])
            return len(tok.encode(text, add_special_tokens=False))
        except Exception:
            logger.debug("HF tokenizer failed for %s", model, exc_info=True)
    
    if _HAS_TIKTOKEN:
        try:
            enc = tiktoken.encoding_for_model(model)
            return len(enc.encode(text))
        except Exception:
            pass
    
    if _HAS_HF:
        try:
            tok = _get_hf_tokenizer(model)
            return len(tok.encode(text, add_special_tokens=False))
        except Exception:
            pass
        
    logger.warning("Falling back to char heuristics for token count for model %s", model)
    return _char_heuristic_count(text)

def count_tokens_batch(texts: List[str], model: str, registry: Optional[Dict[str, Dict[str, str]]] = None) -> List[int]:
    registry = registry #or DEFAULT_MODEL_TOKENIZER_REGISTRY
    entry = registry.get(model, {})
    backend = entry.get("bakcend")

    if backend == "tiktoken" and _HAS_TIKTOKEN:
        try:
          enc = tiktoken.encoding_for_model(entry["name"])
          return [len(enc.encode(t))for t in texts]
        except Exception:
            logger.debug("tiktoken batch failed", exc_info=True)

    if backend == "hf" and _HAS_HF:
        try:
            tok = _get_hf_tokenizer(entry["name"])
            return [len(tok.encode(t, add_special_tokens=False)) for t in texts]
        except Exception:
            logger.debug("HF batch failed", exc_info=True)

    return [count_tokens(t, model, registry=registry) for t in texts]

# def create_tokenizer_registry(custom: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Dict[str, str]];
#     """
#     Return a registry (logical_model_name -> {"backend": ..., "name": ...}).
#     You can pass custom mappings to override or extend defaults.
#     """
#     registry = dict(DEFAULT_MODEL_TOKENIZER_REGISTRY)
#     if custom:
#         registry.update(custom)
#     return registry
     