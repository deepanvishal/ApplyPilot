from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchText

client = QdrantClient(path="C:/Users/Deepan/.pilot-intel/qdrant")

# Scroll for Niagara Bottling jobs
results, _ = client.scroll(
    collection_name="job_descriptions",
    scroll_filter=Filter(
        must=[FieldCondition(key="company", match=MatchText(text="Niagara"))]
    ),
    limit=5,
    with_payload=True,
    with_vectors=False,
)

print(f"Found {len(results)} points\n")
for r in results:
    p = r.payload
    print(f"ID: {r.id}")
    print(f"Company: {p.get('company')}")
    print(f"Title: {p.get('title')}")
    print(f"Site: {p.get('site')}")
    text = p.get("full_description") or p.get("description") or p.get("text") or p.get("chunk") or ""
    print(f"Text ({len(text)} chars): {text[:400]}")
    print("---")
