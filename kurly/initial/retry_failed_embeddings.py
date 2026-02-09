#!/usr/bin/env python3
"""
실패한 Review만 잘라서 재시도 → Qdrant 저장 + 체크포인트 반영
- embedding_failures.json 또는 체크포인트 vs DB 차이로 실패 ID 목록 사용
- contents를 TRUNCATE_LEN 자로 자른 뒤 임베딩 재시도
- 성공 시 Qdrant 저장 및 체크포인트에 추가

사용법:
  python retry_failed_embeddings.py              # embedding_failures.json 기준
  python retry_failed_embeddings.py --from-db    # 체크포인트 vs DB 차이로 실패 ID 추출
"""
import json
import requests
import subprocess
import csv
import io
import argparse
from datetime import datetime

CHECKPOINT_FILE = "embedding_checkpoint.json"
FAILURES_FILE = "embedding_failures.json"
OLLAMA_EMBED_URL = "http://localhost:11434/api/embed"
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "kurly_skin"
TRUNCATE_LEN = 500  # 잘라서 재시도할 최대 글자 수
BGE_M3_DIM = 1024

# Ollama가 계속 500 낼 때 쓰는 대체 벡터 (단위 벡터, cosine 유효)
FALLBACK_VECTOR = [1.0 / (BGE_M3_DIM ** 0.5)] * BGE_M3_DIM


def query_postgres(sql: str):
    cmd = [
        "docker", "exec", "self-hosted-ai-starter-kit-postgres-1",
        "psql", "-U", "root", "-d", "kurly_reviews", "--csv", "-c", sql
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    reader = csv.reader(io.StringIO(result.stdout))
    rows = list(reader)
    return rows[1:] if rows else []


def try_embed(text: str):
    try:
        r = requests.post(
            OLLAMA_EMBED_URL,
            json={"model": "bge-m3", "input": text},
            timeout=30
        )
        r.raise_for_status()
        data = r.json()
        emb = data.get("embeddings")
        if emb and len(emb) > 0:
            return emb[0], None
        return None, "empty embeddings"
    except Exception as e:
        return None, str(e)


def save_to_qdrant(point_id: int, vector: list, payload: dict, retry: int = 3) -> bool:
    url = f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points"
    for attempt in range(retry):
        try:
            resp = requests.put(
                url,
                json={"points": [{"id": point_id, "vector": vector, "payload": payload}]},
                timeout=10
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            if attempt < retry - 1:
                import time
                time.sleep(1)
                continue
            print(f"  ❌ Qdrant 저장 실패: {e}")
            return False
    return False


def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"products": [], "reviews": []}


def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="실패한 Review 잘라서 재시도")
    parser.add_argument("--from-db", action="store_true", help="체크포인트 vs DB 차이로 실패 ID 추출")
    parser.add_argument("--truncate", type=int, default=TRUNCATE_LEN, help=f"잘라낼 최대 글자 수 (기본 {TRUNCATE_LEN})")
    parser.add_argument("--fallback", action="store_true", help="임베딩 실패 시 대체 벡터로 Qdrant 저장 (체크포인트 완료용)")
    args = parser.parse_args()

    if args.from_db:
        checkpoint = load_checkpoint()
        processed = set(checkpoint.get("reviews", []))
        sql = """
            SELECT id FROM kurly_skin_reviews
            WHERE contents IS NOT NULL AND contents != ''
            ORDER BY id
        """
        rows = query_postgres(sql)
        all_ids = {int(r[0]) for r in rows}
        failed_ids = sorted(all_ids - processed)
        print(f"📊 DB vs 체크포인트: 미처리 {len(failed_ids)}개")
    else:
        try:
            with open(FAILURES_FILE) as f:
                failures = json.load(f)
        except FileNotFoundError:
            print(f"❌ {FAILURES_FILE} 없음. 먼저 find_failed_embeddings.py 또는 initial_embedding.py 실행 후 생성되거나, --from-db 사용.")
            return
        failed_ids = [x["review_id"] for x in failures if x.get("review_id")]
        print(f"📂 {FAILURES_FILE}: {len(failed_ids)}개 재시도")

    if not failed_ids:
        print("재시도할 항목 없음.")
        return

    truncate_len = args.truncate
    print(f"✂️  contents를 {truncate_len}자로 잘라서 임베딩 시도")
    if args.fallback:
        print("🔄 임베딩 실패 시 대체 벡터로 저장 (--fallback)")
    print()

    checkpoint = load_checkpoint()
    processed = set(checkpoint.get("reviews", []))
    success = 0
    fail = 0

    for review_id in failed_ids:
        sql = f"""
            SELECT id, product_no, contents, review_score, registered_at
            FROM kurly_skin_reviews WHERE id = {review_id}
        """
        rows = query_postgres(sql)
        if not rows:
            print(f"  Review {review_id}: DB에 없음, 스킵")
            fail += 1
            continue

        row = rows[0]
        contents = (row[2] or "").strip()
        if not contents:
            print(f"  Review {review_id}: contents 없음, 스킵")
            fail += 1
            continue

        truncated = contents[:truncate_len].strip()

        vector, err = try_embed(truncated)
        use_fallback = False
        if not vector:
            if args.fallback:
                vector = FALLBACK_VECTOR
                use_fallback = True
            else:
                print(f"  Review {review_id}: 임베딩 실패 - {err}")
                fail += 1
                continue

        try:
            review_score = int(row[3]) if row[3] else 3
        except (ValueError, TypeError):
            review_score = 3
        registered_at = row[4][:10] if row[4] else None

        payload = {
            "type": "review",
            "review_id": review_id,
            "product_no": str(row[1]),
            "review_score": review_score,
            "registered_at": registered_at,
            "indexed_at": datetime.now().isoformat(),
        }
        if use_fallback:
            payload["embedding_fallback"] = True  # 검색 시 필터링 가능

        point_id = 10000000 + review_id
        if save_to_qdrant(point_id, vector, payload):
            processed.add(review_id)
            checkpoint["reviews"] = list(processed)
            save_checkpoint(checkpoint)
            print(f"  Review {review_id}: ✅ 저장" + (" (대체 벡터)" if use_fallback else f" (잘린 길이 {len(truncated)})"))
            success += 1
        else:
            fail += 1

    print(f"\n✅ 성공: {success}, ❌ 실패: {fail}")


if __name__ == "__main__":
    main()
