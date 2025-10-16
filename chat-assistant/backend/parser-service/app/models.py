from pydantic import BaseModel, Field, AnyUrl
from typing import Optional, List, Dict, Any
from datetime import datetime

class FetchRenderedMessage(BaseModel):
    schema_version: int = Field(1, description="Schema version for backward compatibility")
    correlationId: str
    document_id: str
    url: AnyUrl
    r2_bucket: str
    r2_key_rendered: str
    r2_snapshot_key: Optional[str] = None
    r2_url: AnyUrl
    content_hash: str
    http_status: int
    fetched_at: datetime
    url_hash: str
    domain: str

class ChunkInLine(BaseModel):
    chunk_idx: int
    text: str
    text_hash: str
    token_count: Optional[int]

class ParserToLLMMessage(BaseModel):
    schema_version: int = Field(1, description="Schema version for backward compatibility")
    correlationId: str
    document_id: str
    url: AnyUrl
    domain: str
    content_hash: str
    parsed_at: datetime
    chunks_inline: List[ChunkInLine]
    r2_parsed_prefix: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ParserToEmbedderMessage(BaseModel):
    schema_version: int = Field(1, description="Schema version for backward compatibility")
    correlationId: str
    document_id: str
    r2_parsed_prefix: str
    chunk_count: int
    content_hash: str
    parsed_at: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)
