-- ============================================
-- 컬리 스킨케어 리뷰 데이터베이스 스키마
-- ============================================

-- 데이터베이스 생성 (필요시)
-- CREATE DATABASE kurly_reviews;
-- \c kurly_reviews;

-- ============================================
-- 1. 제품 테이블 (kurly_skin_products)
-- ============================================

CREATE TABLE IF NOT EXISTS kurly_skin_products (
    id SERIAL PRIMARY KEY,
    product_no VARCHAR(50) UNIQUE NOT NULL,
    product_name TEXT NOT NULL,
    review_count INTEGER DEFAULT 0,
    short_description TEXT,
    product_image_url TEXT,
    sales_price INTEGER,
    discounted_price INTEGER,
    product_notice_notices JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_product_no ON kurly_skin_products(product_no);
CREATE INDEX IF NOT EXISTS idx_product_name ON kurly_skin_products(product_name);
CREATE INDEX IF NOT EXISTS idx_created_at ON kurly_skin_products(created_at);

-- 코멘트 추가
COMMENT ON TABLE kurly_skin_products IS '컬리 스킨케어 제품 정보';
COMMENT ON COLUMN kurly_skin_products.product_no IS '제품 고유 번호';
COMMENT ON COLUMN kurly_skin_products.product_name IS '제품명';
COMMENT ON COLUMN kurly_skin_products.review_count IS '전체 리뷰 수';
COMMENT ON COLUMN kurly_skin_products.short_description IS '제품 간단 설명';
COMMENT ON COLUMN kurly_skin_products.product_image_url IS '제품 이미지 URL';
COMMENT ON COLUMN kurly_skin_products.sales_price IS '정가';
COMMENT ON COLUMN kurly_skin_products.discounted_price IS '할인가';
COMMENT ON COLUMN kurly_skin_products.product_notice_notices IS '제품 상세 정보 (JSONB)';

-- ============================================
-- 2. 리뷰 테이블 (kurly_skin_reviews)
-- ============================================

CREATE TABLE IF NOT EXISTS kurly_skin_reviews (
    id SERIAL PRIMARY KEY,
    product_no VARCHAR(50) NOT NULL,
    contents TEXT NOT NULL,
    review_score INTEGER CHECK (review_score BETWEEN 1 AND 5),
    registered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_review_product_no ON kurly_skin_reviews(product_no);
CREATE INDEX IF NOT EXISTS idx_review_score ON kurly_skin_reviews(review_score);
CREATE INDEX IF NOT EXISTS idx_registered_at ON kurly_skin_reviews(registered_at);
CREATE INDEX IF NOT EXISTS idx_review_created_at ON kurly_skin_reviews(created_at);

-- Full-text search 인덱스 (한글 검색용)
CREATE INDEX IF NOT EXISTS idx_review_contents_gin ON kurly_skin_reviews USING gin(to_tsvector('korean', contents));

-- 코멘트 추가
COMMENT ON TABLE kurly_skin_reviews IS '컬리 스킨케어 제품 리뷰';
COMMENT ON COLUMN kurly_skin_reviews.product_no IS '제품 고유 번호 (외래키)';
COMMENT ON COLUMN kurly_skin_reviews.contents IS '리뷰 내용';
COMMENT ON COLUMN kurly_skin_reviews.review_score IS 'AI 감성 분석 점수 (1-5)';
COMMENT ON COLUMN kurly_skin_reviews.registered_at IS '리뷰 등록일';

-- ============================================
-- 3. 외래키 제약조건 (선택사항)
-- ============================================

-- 주의: 외래키를 추가하면 제품이 먼저 저장되어야 리뷰 저장 가능
-- 필요시 아래 주석 제거
-- ALTER TABLE kurly_skin_reviews 
-- ADD CONSTRAINT fk_product_no 
-- FOREIGN KEY (product_no) 
-- REFERENCES kurly_skin_products(product_no) 
-- ON DELETE CASCADE;

-- ============================================
-- 4. 유용한 뷰 (Views)
-- ============================================

-- 제품별 평균 점수 뷰
CREATE OR REPLACE VIEW product_review_stats AS
SELECT 
    p.product_no,
    p.product_name,
    p.review_count as total_reviews,
    COUNT(r.id) as analyzed_reviews,
    ROUND(AVG(r.review_score), 2) as avg_score,
    COUNT(CASE WHEN r.review_score = 5 THEN 1 END) as very_satisfied,
    COUNT(CASE WHEN r.review_score = 4 THEN 1 END) as satisfied,
    COUNT(CASE WHEN r.review_score = 3 THEN 1 END) as neutral,
    COUNT(CASE WHEN r.review_score = 2 THEN 1 END) as dissatisfied,
    COUNT(CASE WHEN r.review_score = 1 THEN 1 END) as very_dissatisfied
FROM kurly_skin_products p
LEFT JOIN kurly_skin_reviews r ON p.product_no = r.product_no
GROUP BY p.product_no, p.product_name, p.review_count;

COMMENT ON VIEW product_review_stats IS '제품별 리뷰 통계 (평균 점수, 감성 분포)';

-- 최근 리뷰 뷰
CREATE OR REPLACE VIEW recent_reviews AS
SELECT 
    r.id,
    p.product_name,
    r.product_no,
    LEFT(r.contents, 100) as preview,
    r.review_score,
    r.registered_at,
    r.created_at
FROM kurly_skin_reviews r
JOIN kurly_skin_products p ON r.product_no = p.product_no
ORDER BY r.created_at DESC;

COMMENT ON VIEW recent_reviews IS '최근 분석된 리뷰 목록';

-- ============================================
-- 5. 샘플 쿼리
-- ============================================

-- 전체 제품 조회
-- SELECT * FROM kurly_skin_products LIMIT 10;

-- 전체 리뷰 조회
-- SELECT * FROM kurly_skin_reviews LIMIT 10;

-- 제품별 평균 점수
-- SELECT * FROM product_review_stats ORDER BY avg_score DESC;

-- 가장 만족도가 높은 제품
-- SELECT 
--     product_name, 
--     avg_score, 
--     analyzed_reviews
-- FROM product_review_stats 
-- WHERE analyzed_reviews >= 10
-- ORDER BY avg_score DESC 
-- LIMIT 10;

-- 특정 키워드가 포함된 리뷰 검색
-- SELECT 
--     product_no, 
--     LEFT(contents, 100) as preview, 
--     review_score 
-- FROM kurly_skin_reviews 
-- WHERE contents LIKE '%촉촉%' 
-- LIMIT 10;

-- 감성별 리뷰 분포
-- SELECT 
--     review_score,
--     COUNT(*) as count,
--     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
-- FROM kurly_skin_reviews
-- GROUP BY review_score
-- ORDER BY review_score DESC;

-- ============================================
-- 6. 데이터베이스 권한 설정 (선택사항)
-- ============================================

-- n8n 전용 사용자 생성 (필요시)
-- CREATE USER n8n_user WITH PASSWORD 'your_secure_password';

-- 권한 부여
-- GRANT SELECT, INSERT, UPDATE ON kurly_skin_products TO n8n_user;
-- GRANT SELECT, INSERT, UPDATE ON kurly_skin_reviews TO n8n_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO n8n_user;

-- ============================================
-- 7. 백업 명령어 (참고용)
-- ============================================

-- 전체 데이터베이스 백업:
-- pg_dump kurly_reviews > kurly_reviews_backup.sql

-- 특정 테이블만 백업:
-- pg_dump kurly_reviews -t kurly_skin_products -t kurly_skin_reviews > kurly_reviews_tables_backup.sql

-- 복원:
-- psql kurly_reviews < kurly_reviews_backup.sql

-- ============================================
-- 설치 완료!
-- ============================================

-- 테이블이 제대로 생성되었는지 확인
SELECT 
    table_name, 
    table_type 
FROM information_schema.tables 
WHERE table_schema = 'public' 
    AND table_name IN ('kurly_skin_products', 'kurly_skin_reviews');

-- 뷰 확인
SELECT 
    table_name 
FROM information_schema.views 
WHERE table_schema = 'public' 
    AND table_name IN ('product_review_stats', 'recent_reviews');

-- 인덱스 확인
SELECT 
    tablename, 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE schemaname = 'public' 
    AND tablename IN ('kurly_skin_products', 'kurly_skin_reviews')
ORDER BY tablename, indexname;
