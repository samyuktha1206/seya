from typing import Dict, Optional
DEFAULT_MODEL_REGISTRY = Dict[str, Dict[str, str]] = {
    "gpt-40": {"backend": "tiktoken", "name": "gpt-40", "context_window": "", "prompt":"", "response":""},
    "gpt-4-32k": {"backend": "tiktoken", "name": "gpt-4-32k", "context_window": "", "prompt":"", "response":""},
    "text-embedding-3-small": {"backend": "tiktoken", "name": "text-embedding-3-small", "context_window": "", "prompt":"", "response":""},
    "all-MiniLM-L6-v2": {"backend": "hf", "name": "all-MiniLM-L6-v2", "context_window": "", "prompt":"", "response":""}
}

def create_tokenizer_registry(custom: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Dict[str, str]]:
    """
    Return a registry (logical_model_name -> {"backend": ..., "name": ...}).
    custom overrides or extends DEFAULT_MODEL_TOKENIZER_REGISTRY.
    """

    registry = Dict(DEFAULT_MODEL_REGISTRY)

    if custom:
        registry.update(custom)
    return custom

def compute_max_chars(llm_model: str, embed_model: str, registry: Dict[str, Dict[str, str]]) -> int:
    return min((registry.get(llm_model).get("context-window")- registry.get(llm_model).get("prompt")-registry.get(llm_model).get("response")), (registry.get(embed_model).get("context-window")- registry.get(embed_model).get("prompt")-registry.get(embed_model).get("response")))*4
