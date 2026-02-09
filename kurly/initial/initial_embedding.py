#!/usr/bin/env python3
"""
Kurly 스킨케어 제품/리뷰 초기 임베딩 스크립트
PostgreSQL → Ollama BGE-M3 → Qdrant

사용법:
    python initial_embedding.py --product  # Product만
    python initial_embedding.py --review   # Review만
    python initial_embedding.py --all      # 전체
    python initial_embedding.py --review --test 10  # 테스트(10개만)
"""

import subprocess
import json
import requests
import argparse
import time
import csv
import io
from datetime import datetime
from typing import List, Dict, Optional
from tqdm import tqdm
import sys

# ============================================================================
# 설정
# ============================================================================

OLLAMA_EMBED_URL = "http://localhost:11434/api/embed"
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "kurly_skin"
BATCH_SIZE = 50
CHECKPOINT_FILE = "embedding_checkpoint.json"

# ============================================================================
# PostgreSQL 쿼리
# ============================================================================

def query_postgres(sql: str) -> List[tuple]:
    """PostgreSQL 쿼리 실행 (docker exec, CSV)"""
    cmd = [
        "docker", "exec", "self-hosted-ai-starter-kit-postgres-1",
        "psql", "-U", "root", "-d", "kurly_reviews",
        "--csv", "-c", sql
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        reader = csv.reader(io.StringIO(result.stdout))
        rows = list(reader)
        return rows[1:] if rows else []
    except subprocess.CalledProcessError as e:
        print(f"❌ PostgreSQL 쿼리 실패: {e}")
        print(f"   Error: {e.stderr}")
        sys.exit(1)


# ============================================================================
# Product Notice → 텍스트 (n8n과 동일)
# ============================================================================

def notice_json_to_text(notices: List[Dict]) -> str:
    """Product notice JSON을 자연어 문장으로 변환"""
    if not notices or not isinstance(notices, list):
        return ""

    sentences = []
    for item in notices:
        title = (item.get("title") or "").strip()
        desc = (item.get("description") or "").strip()
        if not title or not desc:
            continue
        clean_title = title.replace("｢화장품법｣에 따라 ", "").replace("｢화장품법｣에 따른 ", "")
        desc = desc.replace("\\n", " ").strip()
        if "용량" in title or "중량" in title:
            sentences.append(f"내용물의 용량은 {desc}입니다.")
        elif "피부타입" in title or "사양" in title:
            sentences.append(f"{desc}에 사용 가능한 제품입니다.")
        elif "사용방법" in title:
            sentences.append(f"사용 방법은 {desc}.")
        elif "제조국" in title:
            sentences.append(f"제조국은 {desc}입니다.")
        elif "기능성" in title:
            sentences.append(f"{desc} 제품입니다.")
        elif "성분" in title:
            short_desc = desc[:200] + "..." if len(desc) > 200 else desc
            sentences.append(f"주요 성분은 {short_desc}입니다.")
        elif "주의사항" in title:
            sentences.append(f"사용 시 주의사항은 다음과 같습니다. {desc}")
        else:
            sentences.append(f"{clean_title}은 {desc}입니다.")
    return " ".join(sentences)


# ============================================================================
# Ollama 임베딩
# ============================================================================

def generate_embedding(text: str, retry: int = 3) -> tuple:
    """Ollama BGE-M3 임베딩. (vector, error_message) 반환. /api/embed + input 사용."""
    last_error = None
    for attempt in range(retry):
        try:
            response = requests.post(
                OLLAMA_EMBED_URL,
                json={"model": "bge-m3", "input": text},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            emb = data.get("embeddings")
            if emb and len(emb) > 0:
                return (emb[0], None)
            return (None, "empty embeddings in response")
        except Exception as e:
            last_error = e
            if attempt < retry - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"\n❌ 임베딩 생성 실패: {e}")
            return (None, str(e))
    return (None, str(last_error) if last_error else "unknown")


# ============================================================================
# Qdrant 저장
# ============================================================================

def save_to_qdrant(point_id, vector: List[float], payload: Dict, retry: int = 3) -> bool:
    url = f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points"
    for attempt in range(retry):
        try:
            r = requests.put(
                url,
                json={"points": [{"id": point_id, "vector": vector, "payload": payload}]},
                timeout=10
            )
            r.raise_for_status()
            return True
        except Exception as e:
            if attempt < retry - 1:
                time.sleep(1)
                continue
            print(f"\n❌ Qdrant 저장 실패 (ID: {point_id}): {e}")
            return False
    return False


# ============================================================================
# 체크포인트
# ============================================================================

def load_checkpoint() -> Dict:
    try:
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"products": [], "reviews": []}


def save_checkpoint(checkpoint: Dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


# ============================================================================
# Product 처리
# ============================================================================

def process_products(test_limit=None):
    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("products", []))

    sql = f"""
        SELECT id, product_no, product_name, short_description,
               product_notice_notices, sales_price, discounted_price,
               review_count, product_image_url
        FROM kurly_skin_products
        ORDER BY id
        {f'LIMIT {test_limit}' if test_limit else ''}
    """
    print("📊 PostgreSQL에서 Product 조회 중...")
    rows = query_postgres(sql)
    total = len(rows)
    print(f"✅ 총 {total}개 Product 발견")
    if test_limit:
        print(f"🧪 테스트 모드: 최대 {test_limit}개")

    success_count = 0
    skip_count = 0
    fail_count = 0

    with tqdm(total=total, desc="Processing Products", unit="item") as pbar:
        for row in rows:
            product_id = int(row[0])
            if product_id in processed_ids:
                skip_count += 1
                pbar.update(1)
                continue

            try:
                product_no = row[1]
                product_name = row[2] or ""
                short_description = row[3] or ""
                try:
                    notices = json.loads(row[4]) if row[4] else []
                except Exception:
                    notices = []
                sales_price = int(row[5]) if row[5] else 0
                discounted_price = int(row[6]) if row[6] else 0
                review_count = int(row[7]) if row[7] else 0
                product_image_url = row[8] if row[8] else ""

                notice_text = notice_json_to_text(notices)
                embedding_text = f"상품명: {product_name}\n설명: {short_description}\n\n{notice_text}"

                vector, _ = generate_embedding(embedding_text)
                if not vector:
                    fail_count += 1
                    pbar.update(1)
                    continue

                payload = {
                    "type": "product",
                    "product_no": str(product_no),
                    "product_name": product_name,
                    "sales_price": sales_price,
                    "discounted_price": discounted_price,
                    "review_count": review_count,
                    "product_image_url": product_image_url,
                    "price": discounted_price if discounted_price > 0 else sales_price,
                    "has_discount": discounted_price > 0 and discounted_price < sales_price,
                    "indexed_at": datetime.now().isoformat()
                }

                point_id = int(product_no)
                if save_to_qdrant(point_id, vector, payload):
                    success_count += 1
                    processed_ids.add(product_id)
                    if success_count % 10 == 0:
                        checkpoint["products"] = list(processed_ids)
                        save_checkpoint(checkpoint)
                else:
                    fail_count += 1
            except Exception as e:
                print(f"\n❌ Product 처리 중 에러! Product ID: {product_id}, 에러: {e}")
                fail_count += 1
            pbar.update(1)

    checkpoint["products"] = list(processed_ids)
    save_checkpoint(checkpoint)
    print("\n" + "=" * 70)
    print("📦 Product 처리 완료!")
    print(f"   ✅ 성공: {success_count}, ⏭️ 스킵: {skip_count}, ❌ 실패: {fail_count}")
    print("=" * 70)


# ============================================================================
# Review 처리
# ============================================================================

def process_reviews(test_limit=None):
    checkpoint = load_checkpoint()
    processed_ids = set(checkpoint.get("reviews", []))

    sql = f"""
        SELECT id, product_no, contents, review_score, registered_at
        FROM kurly_skin_reviews
        WHERE contents IS NOT NULL AND contents != ''
        ORDER BY id
        {f'LIMIT {test_limit}' if test_limit else ''}
    """
    print("📊 PostgreSQL에서 Review 조회 중...")
    rows = query_postgres(sql)
    total = len(rows)
    print(f"✅ 총 {total}개 Review 발견")
    if test_limit:
        print(f"🧪 테스트 모드: 최대 {test_limit}개")

    success_count = 0
    skip_count = 0
    fail_count = 0
    failed_log = []

    with tqdm(total=total, desc="Processing Reviews", unit="item") as pbar:
        for row in rows:
            review_id = int(row[0])
            if review_id in processed_ids:
                skip_count += 1
                pbar.update(1)
                continue

            try:
                product_no = row[1]
                contents = (row[2] or "").strip()
                try:
                    review_score = int(row[3]) if row[3] else 3
                except (ValueError, TypeError):
                    review_score = 3
                registered_at = row[4][:10] if row[4] else None

                if not contents:
                    skip_count += 1
                    pbar.update(1)
                    continue

                vector, embed_error = generate_embedding(contents)
                if not vector:
                    fail_count += 1
                    failed_log.append({
                        "review_id": review_id,
                        "product_no": row[1],
                        "error": embed_error,
                        "contents_preview": contents[:100].replace("\n", " "),
                        "contents_length": len(contents),
                    })
                    pbar.update(1)
                    continue

                payload = {
                    "type": "review",
                    "review_id": review_id,
                    "product_no": str(product_no),
                    "review_score": review_score,
                    "registered_at": registered_at,
                    "indexed_at": datetime.now().isoformat(),
                    "contents": contents[:500] if len(contents) <= 500 else contents[:500] + "…",
                }

                point_id = 10000000 + review_id
                if save_to_qdrant(point_id, vector, payload):
                    success_count += 1
                    processed_ids.add(review_id)
                    if success_count % 50 == 0:
                        checkpoint["reviews"] = list(processed_ids)
                        save_checkpoint(checkpoint)
                else:
                    fail_count += 1
            except Exception as e:
                print(f"\n❌ Review 처리 중 에러! Review ID: {review_id}, 에러: {e}")
                fail_count += 1
            pbar.update(1)

    checkpoint["reviews"] = list(processed_ids)
    save_checkpoint(checkpoint)
    if failed_log:
        with open("embedding_failures.json", "w", encoding="utf-8") as f:
            json.dump(failed_log, f, ensure_ascii=False, indent=2)
        print(f"\n📄 실패 상세 로그: embedding_failures.json ({len(failed_log)}건)")

    print("\n" + "=" * 70)
    print("📝 Review 처리 완료!")
    print(f"   ✅ 성공: {success_count}, ⏭️ 스킵: {skip_count}, ❌ 실패: {fail_count}")
    print("=" * 70)


# ============================================================================
# 메인
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kurly 스킨케어 초기 임베딩")
    parser.add_argument("--product", action="store_true", help="Product만")
    parser.add_argument("--review", action="store_true", help="Review만")
    parser.add_argument("--all", action="store_true", help="전체")
    parser.add_argument("--test", type=int, metavar="N", help="테스트 모드(N개만)")
    args = parser.parse_args()

    if not (args.product or args.review or args.all):
        args.all = True

    print("\n" + "🚀 " * 20)
    print("   Kurly 스킨케어 초기 임베딩 스크립트")
    print("🚀 " * 20)
    print(f"\n⏰ 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start = time.time()
    try:
        if args.all or args.product:
            process_products(test_limit=args.test)
        if args.all or args.review:
            process_reviews(test_limit=args.test)
        elapsed = time.time() - start
        print(f"\n✨ 전체 완료! 소요: {elapsed:.1f}초 ({elapsed/60:.1f}분)")
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자 중단. 체크포인트 저장됨. 다시 실행하면 이어서 진행됩니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 에러: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
