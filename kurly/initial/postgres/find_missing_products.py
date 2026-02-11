#!/usr/bin/env python3
"""
JSON 파일과 PostgreSQL을 비교하여 
부족한 제품을 찾고 INSERT SQL을 생성하는 스크립트
"""

import json
import subprocess
from datetime import datetime

def load_json_products():
    """JSON 파일에서 제품 목록 로드"""
    with open('data/kurly_skin_products_merged.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    products = {}
    for item in data:
        products[str(item['product_no'])] = {
            'product_no': item['product_no'],
            'name': item['name'],
            'review_count': item.get('review_count', 0),
            'short_description': item.get('short_description', ''),
            'product_vertical_medium_url': item.get('product_vertical_medium_url', ''),
            'sales_price': item.get('sales_price', 0),
            'discounted_price': item.get('discounted_price', 0),
            'product_notice_notices': item.get('product_notice_notices', [])
        }
    
    return products

def get_existing_products():
    """PostgreSQL에서 기존 제품 조회 (Docker 사용)"""
    try:
        result = subprocess.run([
            'docker', 'exec', '-i', 'self-hosted-ai-starter-kit-postgres-1',
            'psql', '-U', 'root', '-d', 'kurly_reviews', '-t', '-c',
            'SELECT product_no FROM kurly_skin_products'
        ], capture_output=True, text=True, check=True)
        
        existing = set()
        for line in result.stdout.strip().split('\n'):
            product_no = line.strip()
            if product_no:
                existing.add(product_no)
        
        return existing
    except subprocess.CalledProcessError as e:
        raise Exception(f"PostgreSQL 조회 실패: {e.stderr}")

def generate_insert_sql(missing_products):
    """부족한 제품들의 INSERT SQL 생성"""
    
    sql_statements = []
    
    for product_no, product in missing_products.items():
        # JSON 이스케이프 처리
        name = product['name'].replace("'", "''")
        desc = product['short_description'].replace("'", "''") if product['short_description'] else ''
        notices_json = json.dumps(product['product_notice_notices'], ensure_ascii=False)
        
        sql = f"""
INSERT INTO kurly_skin_products 
  (product_no, product_name, review_count, short_description, 
   product_image_url, sales_price, discounted_price, product_notice_notices, updated_at)
VALUES 
  ('{product_no}', 
   '{name}', 
   {product['review_count']}, 
   '{desc}', 
   '{product['product_vertical_medium_url']}', 
   {product['sales_price']}, 
   {product['discounted_price']}, 
   $${notices_json}$$::jsonb, 
   CURRENT_TIMESTAMP);
"""
        sql_statements.append(sql.strip())
    
    return sql_statements

def main():
    print("=" * 80)
    print("부족한 제품 찾기 및 INSERT SQL 생성")
    print("=" * 80)
    print()
    
    # 1. JSON 파일에서 제품 로드
    print("📂 JSON 파일 읽는 중...")
    json_products = load_json_products()
    print(f"   - JSON 파일 제품 수: {len(json_products)}개")
    print()
    
    # 2. PostgreSQL에서 기존 제품 조회
    print("🗄️  PostgreSQL 조회 중...")
    try:
        existing_products = get_existing_products()
        print(f"   - DB 제품 수: {len(existing_products)}개")
        print()
    except Exception as e:
        print(f"   ❌ DB 연결 실패: {e}")
        print()
        print("   수동으로 확인하려면:")
        print("   docker exec -it self-hosted-ai-starter-kit-postgres-1 \\")
        print("     psql -U root -d kurly_reviews -c 'SELECT COUNT(*) FROM kurly_skin_products;'")
        return
    
    # 3. 부족한 제품 찾기
    print("🔍 부족한 제품 찾는 중...")
    missing_product_nos = set(json_products.keys()) - existing_products
    
    if not missing_product_nos:
        print("   ✅ 모든 제품이 DB에 존재합니다!")
        return
    
    missing_products = {no: json_products[no] for no in missing_product_nos}
    print(f"   - 부족한 제품 수: {len(missing_products)}개")
    print()
    
    # 4. 부족한 제품 목록 출력
    print("📋 부족한 제품 목록:")
    print("-" * 80)
    for i, (product_no, product) in enumerate(missing_products.items(), 1):
        print(f"{i:2d}. [{product_no}] {product['name']}")
    print()
    
    # 5. INSERT SQL 생성
    print("📝 INSERT SQL 생성 중...")
    sql_statements = generate_insert_sql(missing_products)
    
    # 6. SQL 파일로 저장
    output_file = 'insert_missing_products.sql'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- 부족한 제품 INSERT SQL\n")
        f.write(f"-- 생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- 총 {len(sql_statements)}개 제품\n\n")
        f.write("BEGIN;\n\n")
        f.write("\n\n".join(sql_statements))
        f.write("\n\nCOMMIT;\n")
    
    print(f"   ✅ SQL 파일 저장: {output_file}")
    print()
    
    # 7. 실행 방법 안내
    print("=" * 80)
    print("🚀 실행 방법:")
    print("=" * 80)
    print()
    print("1. SQL 파일 확인:")
    print(f"   cat {output_file}")
    print()
    print("2. PostgreSQL에 실행:")
    print("   docker exec -i self-hosted-ai-starter-kit-postgres-1 \\")
    print(f"     psql -U root -d kurly_reviews < {output_file}")
    print()
    print("3. 또는 수동으로 복사-붙여넣기:")
    print("   docker exec -it self-hosted-ai-starter-kit-postgres-1 \\")
    print("     psql -U root -d kurly_reviews")
    print(f"   그 다음 {output_file} 내용을 복사해서 붙여넣기")
    print()

if __name__ == "__main__":
    main()
