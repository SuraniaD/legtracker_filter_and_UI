"""
Embeddings through OpenRouter, same model and input truncation as the old
LegTracker (openai/text-embedding-3-small, 1536 dims, input cut at 8,000 chars).

Differences from the old single-text helper, none of which change the vectors:
  - texts are sent in batches (fewer requests); if the endpoint rejects a list
    input, it falls back to one text per request,
  - 429/5xx/network errors are retried,
  - a failure raises EmbeddingError instead of returning None, so callers can
    retry the bill later rather than treating "no vector" as "not relevant",
  - an in-process cache avoids re-embedding identical text within a run.
"""
import hashlib
import os
import time

import httpx

EMBEDDING_MODEL = "openai/text-embedding-3-small"
EMBEDDING_DIMS = 1536
MAX_INPUT_CHARS = 8000
BATCH_SIZE = 64
MAX_RETRIES = 5


class EmbeddingError(Exception):
    pass


def make_http_client() -> httpx.Client:
    """Factory so tests can swap in a mock transport."""
    return httpx.Client(timeout=60.0)


class Embedder:
    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise EmbeddingError("Set OPENROUTER_API_KEY.")
        self.url = (base_url or os.environ.get("OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1").rstrip("/") + "/embeddings"
        self.client = make_http_client()
        self.cache: dict[str, list[float]] = {}
        self.requests = 0
        self._batch_ok = True

    @staticmethod
    def _key(text: str) -> str:
        return hashlib.sha256(text.strip().encode()).hexdigest()

    def _post(self, inputs):
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json",
                   "X-Title": "AnimalLegislationTracker"}
        payload = {"model": EMBEDDING_MODEL, "input": inputs}
        for attempt in range(1, MAX_RETRIES + 1):
            self.requests += 1
            try:
                r = self.client.post(self.url, headers=headers, json=payload)
            except httpx.TransportError as exc:
                if attempt == MAX_RETRIES:
                    raise EmbeddingError(f"network error: {type(exc).__name__}") from None
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if attempt == MAX_RETRIES:
                    raise EmbeddingError(f"HTTP {r.status_code} after {MAX_RETRIES} attempts")
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 400:
                raise EmbeddingError(f"HTTP {r.status_code}: {r.text[:200]}")
            data = r.json().get("data") or []
            if len(data) != (len(inputs) if isinstance(inputs, list) else 1):
                raise EmbeddingError(f"expected {len(inputs) if isinstance(inputs, list) else 1} vectors, got {len(data)}")
            # order by the returned index, as the API allows any order
            data = sorted(data, key=lambda d: d.get("index", 0))
            vectors = [d["embedding"] for d in data]
            if any(len(v) != EMBEDDING_DIMS for v in vectors):
                raise EmbeddingError(f"unexpected vector size {len(vectors[0])}")
            return vectors
        raise EmbeddingError("gave up")

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        """Vectors for `texts`, in order. Raises EmbeddingError on failure."""
        cut = [t[:MAX_INPUT_CHARS] for t in texts]
        todo = [t for t in dict.fromkeys(cut) if self._key(t) not in self.cache]
        for i in range(0, len(todo), BATCH_SIZE):
            chunk = todo[i:i + BATCH_SIZE]
            if self._batch_ok and len(chunk) > 1:
                try:
                    vecs = self._post(chunk)
                except EmbeddingError as exc:
                    if "HTTP 4" in str(exc):     # list input not accepted: go one by one
                        self._batch_ok = False
                        vecs = [self._post(t)[0] for t in chunk]
                    else:
                        raise
            else:
                vecs = [self._post(t)[0] for t in chunk]
            for t, v in zip(chunk, vecs):
                self.cache[self._key(t)] = v
        return [self.cache[self._key(t)] for t in cut]
