-- 특정 날짜에 생성된 제품만 삭제 (해당 날짜 워크플로로 들어간 제품만 제거할 때)
-- 도커에서: docker exec -it <postgres_container> psql -U root -d kurly_reviews -f - < 이파일
-- 또는 psql 들어가서 아래 한 줄 실행 (날짜만 필요에 따라 수정)

DELETE FROM kurly_skin_products
WHERE created_at::date = '2026-02-10';

DELETE FROM kurly_skin_products
WHERE created_at::date = '2026-02-10';

-- self-hosted-ai-starter-kit-postgres-1

-- docker exec -it self-hosted-ai-starter-kit-postgres-1 psql -U root -d kurly_reviews -c "DELETE FROM kurly_skin_reviews WHERE created_at::date = '2026-02-10'; DELETE FROM kurly_skin_products WHERE created_at::date = '2026-02-10';"