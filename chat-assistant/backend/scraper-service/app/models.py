from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, AnyUrl, ConfigDict

class SearchResultEvent(BaseModel):

  model_config = ConfigDict(extra = "ignore")

  correlationId: str
  query: Optional[str] = None
  sourceDomain: Optional[str] = None
  link: AnyUrl
  title: Optional[str] = None
  snippet: Optional[str] = None
  rank: Optional[int] = Field(None, ge=1)
  FetchedAtMs: Optional[int] = Field(None, ge=0)

class ScraperFetchedEvent(BaseModel):

  model_config = ConfigDict(extra = "ignore")

  correlationId: str
  document_id: str | None = None
  url: AnyUrl
  r2_bucket: str
  r2_key_raw: str
  r2_url: Optional[str] = None
  content_hash: str
  http_status: int = Field(..., ge=100, le=599)
  fetched_at: str
  sourceDomain: Optional[str] = None
  title: Optional[str] = None
  url_hash: str
  done: bool = True