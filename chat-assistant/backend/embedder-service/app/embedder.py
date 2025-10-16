# app/embedder.py
import asyncio
from typing import List, Dict, Tuple
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
import pinecone
from app.config import settings

# Initialize Pinecone if configured
if settings.PINECONE_API_KEY and settings.PINECONE_ENVIRONMENT:
    pinecone.init(api_key=settings.PINECONE_API_KEY, environment=settings.PINECONE_ENVIRONMENT)


class EmbedderService:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        print(f"[EmbedderService] Loading {self.model_name} on {self.device} ...")
        self.model = SentenceTransformer(self.model_name, device=self.device)

        # Pinecone index (optional)
        self.index = None
        if settings.PINECONE_INDEX_NAME:
            self.index = pinecone.Index(settings.PINECONE_INDEX_NAME)

        # safe batch size
        self.batch_size = 64 if self.device == "cpu" else 256

    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Async-friendly embedding with batching."""
        if not texts:
            return []

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._embed_sync, texts)

    def _embed_sync(self, texts: List[str]) -> List[List[float]]:
        """Batch encode texts and normalize embeddings."""
        embs = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            with torch.no_grad():
                v = self.model.encode(batch, convert_to_numpy=True, normalize_embeddings=True)
            embs.extend(v.tolist())
        return embs

    async def fetch_existing_checksums(self, ids: List[str]) -> Dict[str, str]:
        """Check which IDs exist in Pinecone and return their checksum metadata."""
        if not self.index or not ids:
            return {}
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._fetch_sync, ids)

    def _fetch_sync(self, ids: List[str]) -> Dict[str, str]:
        resp = self.index.fetch(ids=ids)
        result = {}
        for _id, val in (resp.get("vectors") or {}).items():
            md = val.get("metadata") or {}
            result[_id] = md.get("checksum")
        return result

    async def upsert_embeddings(self, items: List[Tuple[str, List[float], Dict]]):
        """Upsert embeddings to Pinecone."""
        if not self.index:
            raise RuntimeError("Pinecone index not configured")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._upsert_sync, items)

    def _upsert_sync(self, items: List[Tuple[str, List[float], Dict]]):
        # Pinecone supports up to ~1000 vectors per request
        for i in range(0, len(items), 1000):
            batch = items[i:i + 1000]
            self.index.upsert(vectors=batch)
