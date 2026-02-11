import json
from collections import Counter

# JSON 파일 로드
with open('/home/julia/workspace/shopping_project/data/kurly_skin_products_merged.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 모든 리뷰 수집
all_reviews = []
for product in data:
    product_no = product.get('product_no')
    for review in product.get('reviews', []):
        key = (
            str(product_no),
            review.get('contents', ''),
            review.get('registeredAt', '')
        )
        all_reviews.append(key)

# 중복 확인
print(f"총 리뷰 수: {len(all_reviews)}")
print(f"고유 리뷰 수: {len(set(all_reviews))}")
print(f"중복 수: {len(all_reviews) - len(set(all_reviews))}")

# 중복된 리뷰 찾기
duplicates = [item for item, count in Counter(all_reviews).items() if count > 1]
print(f"\n중복된 조합 수: {len(duplicates)}")

if len(duplicates) > 0:
    print("\n중복 예시 (처음 5개):")
    for i, dup in enumerate(duplicates[:5]):
        print(f"\n{i+1}. Product: {dup[0]}")
        print(f"   Content: {dup[1][:50]}...")
        print(f"   Date: {dup[2]}")
        count = Counter(all_reviews)[dup]
        print(f"   중복 횟수: {count}회")
