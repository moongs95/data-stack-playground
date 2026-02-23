# 📋 컬리 스킨케어 일일 크롤링 & 임베딩 워크플로우 가이드

매일 정각 12:00 (KST)에 자동 실행되는 n8n 워크플로우입니다.

## 🎯 워크플로우 개요

이 워크플로우는 **증분(delta)** 기준으로 동작합니다:

1. **Cron 트리거** - 매일 12:00 (Asia/Seoul) 자동 실행
2. **증분 크롤링** - n8n **HTTP Request**로 크롤러 트리거 호출 → `kurly_skin_crawler.py --incremental` 실행. 트리거가 delta JSON을 **응답 body**로 돌려줌 (n8n이 파일 시스템 접근 불필요).
3. **Delta 파싱** - 응답의 `delta` 사용, 신규 데이터 여부 분기
4. **리뷰 감성 평가** - 신규 리뷰를 10개씩 묶어 Ollama **qwen2.5:3b**로 1~5점 감성 분석 후 `review_score` 부여
5. **Postgres** - 제품은 UPSERT, 리뷰는 (점수 포함) INSERT
6. **임베딩** - delta 제품/리뷰만 Ollama BGE-M3로 임베딩 후 Qdrant upsert
7. **Slack 알림** - 실행 결과 전송

---

## 📦 사전 요구사항

### 1. Docker 컨테이너 실행 확인

다음 컨테이너들이 실행 중이어야 합니다:

```bash
# Postgres
docker ps | grep postgres

# Ollama (bge-m3 모델)
docker ps | grep ollama
# 모델 확인: curl http://localhost:11434/api/tags

# Qdrant
docker ps | grep qdrant

# n8n (self-hosted)
docker ps | grep n8n
```

### 2. Postgres 스키마 확인

다음 테이블이 존재해야 합니다:

- `kurly_skin_products` (기준 컬럼: `product_no`)
- `kurly_skin_reviews` (기준 컬럼: `registered_at`)

스키마 생성:
```bash
docker exec -i <postgres_container> psql -U root -d kurly_reviews < script/postgres/database_setup.sql
```

### 3. Qdrant Collection 확인

`kurly_skin` collection이 생성되어 있어야 합니다:

```bash
curl http://localhost:6333/collections/kurly_skin
```

Collection 생성:
```bash
bash script/qdrant/setup_qdrant_collection.sh
```

### 4. 크롤러 및 DB 연결 (.env 로 통일)

**환경 변수는 프로젝트 루트의 `.env` 한 곳에서 관리합니다.**

- `.env` 없으면 `.env.example` 을 복사해 만든 뒤, `DATABASE_URL` 등 실제 값으로 수정하세요.
  ```bash
  cp .env.example .env
  # .env 편집: DATABASE_URL=postgresql://USER:PASSWORD@postgres:5432/kurly_reviews
  ```
- 크롤러·트리거 서비스·스크립트는 모두 이 `.env` 를 참조합니다 (Docker는 compose `env_file`, 호스트 실행 시에는 `python-dotenv` 로 로드).

#### 4-1. (중요) 크롤러 트리거 서비스 실행

n8n에서 **Execute Command** 노드를 쓸 수 없는 환경이므로, 크롤러는 **HTTP 트리거 서비스**로 실행합니다.

**방법 A: Docker Compose (권장)**

1. **메인 compose**(postgres, n8n 등)가 이미 떠 있는 상태에서
2. **프로젝트 루트**에 `.env` 가 있고, 그 안에 `DATABASE_URL` 이 **같은 Docker 네트워크 기준**으로 설정되어 있어야 합니다.  
   예: `DATABASE_URL=postgresql://root:password@postgres:5432/kurly_reviews` (호스트는 `postgres`)
3. 프로젝트 루트에서:
   ```bash
   docker compose -f docker-compose.crawler-trigger.yml up -d
   ```
   (`export` 없이 `.env` 만 있으면 됩니다.)

- 네트워크가 `self-hosted-ai-starter-kit_demo` 등 다른 이름이면 `docker-compose.crawler-trigger.yml` 의 `networks.demo.name` 을 해당 이름으로 맞추세요.
- n8n이 **Docker**면 Execute Crawler 노드 URL: `http://crawler-trigger:8008/crawl/incremental`
- n8n이 **호스트**면 URL: `http://localhost:8008/crawl/incremental`

**방법 B: 호스트에서 직접 실행**

`.env` 가 있으면 별도 `export` 없이 실행됩니다.

```bash
cd /home/julia/workspace/shopping_project
python3 script/crawler_trigger_server.py --host 0.0.0.0 --port 8008
```

(호스트에서 Postgres 접속 시 `.env` 의 `DATABASE_URL` 호스트를 `localhost` 로 두면 됩니다.)

**헬스체크:** `curl http://localhost:8008/health`

증분 크롤링 테스트 (호스트에서 실행 시 `.env` 자동 로드):
```bash
cd /home/julia/workspace/shopping_project
python3 kurly_skin_crawler.py --incremental
```

출력 파일 확인:
```bash
ls -lh data/kurly_skin_delta_*.json
```

---

## 🚀 워크플로우 Import 및 설정

### 1. 워크플로우 Import

1. n8n UI 접속 (http://localhost:5678)
2. 좌측 상단 **"Workflows"** 메뉴 클릭
3. 우측 상단 **"Add workflow"** 드롭다운 → **"Import from file"** 선택
4. 다음 파일 선택:
   ```
   /home/julia/workspace/shopping_project/n8n_workflows/매일 크롤링 데이터 적재/daily_crawl_with_embedding_workflow.json
   ```
5. **"Import"** 클릭

### 2. Credentials 설정

#### Postgres Credential

1. 워크플로우 편집 화면에서 **Postgres 노드** 클릭
2. **Credential** 드롭다운에서 **"Create New Credential"** 선택
3. 다음 정보 입력:
   - **Host**: `localhost` (또는 Postgres 컨테이너 호스트)
   - **Port**: `5432`
   - **Database**: `kurly_reviews`
   - **User**: `root` (또는 설정한 사용자)
   - **Password**: 설정한 비밀번호
4. **"Save"** 클릭
5. 모든 Postgres 노드에 동일한 Credential 적용

#### Slack Credential

Slack 설정은 `SLACK_SETUP_GUIDE.md` 파일을 참고하세요.

**중요**: 다음 노드들에 Slack Credential을 설정해야 합니다:
- **Slack Success Notification**
- **Slack Error Notification**

---

## 🔧 노드별 상세 설명

### 1. Daily Cron (12:00 KST)

**타입**: Schedule Trigger  
**설정**: 매일 12:00 (Asia/Seoul) 자동 실행

```json
{
  "cronExpression": "0 0 * * *",
  "timezone": "Asia/Seoul"
}
```

### 2. Init Workflow

**타입**: Code  
**기능**: 워크플로우 시작 시간 및 날짜 기록

### 3. Execute Crawler

**타입**: Execute Command  
**명령어**:
```bash
cd /home/julia/workspace/shopping_project && python3 kurly_skin_crawler.py --incremental
```

**필수**: 프로젝트 루트 `.env` 에 `DATABASE_URL` 설정 (Postgres 연결).  
**출력**: `data/kurly_skin_delta_YYYY-MM-DD.json` (products, reviews, stats, 실행일 기준)

### 4. Read Delta JSON

**타입**: Read Binary File  
**경로**: `/home/julia/workspace/shopping_project/data/kurly_skin_delta_YYYY-MM-DD.json` (n8n 실행일로 자동 치환)

### 5. Parse Delta

**타입**: Code  
**기능**: delta JSON 파싱, `stats`를 staticData에 저장, `hasNewData` 분기용 데이터 출력

### 6. Check Has New Data

**타입**: IF  
**조건**: `hasNewData === true`

- **True**: Postgres 적재 진행
- **False**: Slack 알림 (새 데이터 없음)

### 10. Split New Products / Split New Reviews

**타입**: Code  
**기능**: Delta의 products/reviews 배열을 개별 아이템으로 분리. 리뷰는 **Split Review Batches** → **Prepare Batch Prompt** → **Ollama Review Analysis (qwen2.5:3b)** → **Parse Batch Results** 를 거쳐 감성 점수(1~5) 부여 후 Insert New Reviews로 전달됩니다.

### 11. Insert New Products

**타입**: Postgres (Insert)  
**테이블**: `kurly_skin_products`  
**컬럼**: `product_no, product_name, review_count, short_description, product_image_url, sales_price, discounted_price, product_notice_notices`

### 12. Insert New Reviews

**타입**: Postgres (Insert)  
**테이블**: `kurly_skin_reviews`  
**컬럼**: `product_no, contents, registered_at, review_score`

- **`registered_at`** = 컬리 사이트에 리뷰가 **원래 등록된 날짜** (고객이 작성한 시점).
- **`created_at`** = 우리 DB에 **기록이 들어온 날짜** (워크플로가 INSERT한 시점).
- 증분 크롤링은 “우리 DB에 없는 **새 리뷰**”만 가져옵니다. **해당 제품이 DB에 처음 들어오는 경우**(신규 제품) 그 제품의 리뷰를 **전부** 가져오기 때문에, 12월·1월에 작성된 리뷰(registered_at)도 오늘 한꺼번에 적재되어 created_at만 오늘로 찍힙니다. 그래서 “갑자기 1월·12월이 나온다”고 보이는 것이며, 정상 동작입니다.

### 13. Collect Postgres Stats

**타입**: Code  
**기능**: Postgres 적재 완료 후 통계 수집

### 14. Collect New Product Nos

**타입**: Code  
**기능**: 신규로 적재된 product_no 목록 수집 (임베딩용)

### 15. Get Products For Embedding

**타입**: Postgres  
**쿼리**: 신규로 적재된 product 조회

### 16. Get Reviews For Embedding

**타입**: Postgres  
**쿼리**:
```sql
SELECT id, product_no, contents, registered_at 
FROM kurly_skin_reviews 
WHERE registered_at >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY registered_at DESC
```

**목적**: 최근 1일 이내 등록된 review 조회 (임베딩용)

### 17. Prepare Product Embeddings

**타입**: Code  
**기능**:
- Product Notice를 자연어 텍스트로 변환 (`initial_embedding.py` 참고)
- 임베딩용 텍스트 생성: `상품명: {name}\n설명: {description}\n\n{notice_text}`
- Qdrant payload 준비

**출력**:
```json
{
  "id": 1000319181,
  "text": "상품명: [달바] 화이트 트러플...",
  "payload": {
    "type": "product",
    "product_no": "1000319181",
    "product_name": "...",
    "sales_price": 59800,
    "discounted_price": 33900,
    ...
  }
}
```

### 18. Prepare Review Embeddings

**타입**: Code  
**기능**:
- Review 내용을 임베딩용 텍스트로 준비
- Qdrant payload 준비 (point_id = 10000000 + review_id)

### 19. Generate Product Embeddings / Generate Review Embeddings

**타입**: HTTP Request  
**URL**: `http://localhost:11434/api/embed`  
**Method**: POST  
**Body**:
```json
{
  "model": "bge-m3",
  "input": "{text}"
}
```

**응답**:
```json
{
  "embeddings": [[0.123, 0.456, ...]]
}
```

### 20. Process Product Embedding / Process Review Embedding

**타입**: Code  
**기능**: Ollama 응답에서 벡터 추출 및 Qdrant 저장 준비

### 21. Format Product Point / Format Review Point

**타입**: Code  
**기능**: Qdrant upsert 형식으로 변환

### 22. Upsert Product To Qdrant / Upsert Review To Qdrant

**타입**: HTTP Request  
**URL**: `http://localhost:6333/collections/kurly_skin/points`  
**Method**: PUT  
**Body**:
```json
{
  "points": [{
    "id": 1000319181,
    "vector": [0.123, 0.456, ...],
    "payload": {...}
  }]
}
```

### 23. Collect Embedding Stats

**타입**: Code  
**기능**: 임베딩 완료 통계 수집 (성공/실패 개수)

### 24. Slack Success Notification

**타입**: Slack  
**기능**: 성공 시 Slack 알림 전송

**메시지 예시**:
```
🎉 *컬리 스킨케어 데이터 크롤링 완료!*

📅 *날짜:* 2026-02-06

📊 *오늘의 업데이트:*
• 🆕 새로운 제품: *3개*
• 💬 새로운 리뷰: *127개*
• 📦 전체 제품 수: *276개*

✅ *PostgreSQL 적재:* 성공
✅ *Qdrant 임베딩:*
  - Product: 3개 성공, 0개 실패
  - Review: 127개 성공, 0개 실패
```

### 25. Slack Error Notification

**타입**: Slack  
**기능**: 실패 시 Slack 알림 전송

**메시지 예시**:
```
❌ *컬리 스킨케어 크롤링 실패!*

📅 *날짜:* 2026-02-06
⏰ *시간:* 00:15:32

🚨 *오류 유형:* 크롤러 실행 실패
📍 *오류 발생 위치:* Execute Crawler

💥 *오류 메시지:*
```
Command failed with exit code 1
```

⚠️ 워크플로우를 확인하고 다시 실행해주세요!
```

---

## 🔍 증분 판단 로직 상세

### Product 증분 판단

1. **기준 컬럼**: `product_no`
2. **로직**:
   - Postgres에서 기존 `product_no` 목록 조회
   - 크롤링된 데이터의 `product_no`와 비교
   - 기존에 없는 `product_no`만 신규로 판단

### Review 증분 판단

1. **기준 컬럼**: `registered_at`
2. **로직**:
   - Postgres에서 기존 review 목록 조회 (`product_no`, `contents`, `registered_at`)
   - 크롤링된 review와 비교 (조합 키: `product_no|contents|registered_at`)
   - 기존에 없는 조합만 신규로 판단

**주의**: Review는 `registered_at` 기준으로 필터링하되, 실제 중복 체크는 `product_no + contents + registered_at` 조합으로 수행합니다.

---

## 🧪 테스트 방법

### 1. 수동 실행 테스트

1. 워크플로우 편집 화면에서 **"Execute Workflow"** 클릭
2. 각 노드의 실행 결과 확인
3. Postgres에서 데이터 확인:
   ```sql
   SELECT COUNT(*) FROM kurly_skin_products;
   SELECT COUNT(*) FROM kurly_skin_reviews;
   ```
4. Qdrant에서 데이터 확인:
   ```bash
   curl http://localhost:6333/collections/kurly_skin/points/scroll
   ```

### 2. Cron 트리거 테스트

1. Cron 표현식을 임시로 변경 (예: `*/5 * * * *` - 5분마다)
2. 워크플로우를 **Active** 상태로 전환
3. 5분 후 자동 실행 확인
4. 원래 시간으로 복구 (`0 0 * * *`)

### 3. 증분 로직 테스트

1. 기존 데이터가 있는 상태에서 크롤러 실행
2. 워크플로우 실행
3. 신규 데이터만 적재되는지 확인:
   ```sql
   -- 신규 product 확인
   SELECT product_no, product_name, created_at 
   FROM kurly_skin_products 
   ORDER BY created_at DESC 
   LIMIT 10;
   
   -- 신규 review 확인
   SELECT id, product_no, registered_at, created_at 
   FROM kurly_skin_reviews 
   ORDER BY created_at DESC 
   LIMIT 10;
   ```

### 수동 1회 실행 검증 (Postgres / Qdrant / Slack)

1. **환경 확인**: `.env` 의 `DATABASE_URL`, Postgres/Ollama/Qdrant 실행, n8n Credentials(Postgres, Slack) 설정
2. n8n에서 해당 워크플로우 열기 → **Execute Workflow** (또는 Test run) 1회 실행
3. **검증**:
   - **Postgres**: `kurly_skin_products` / `kurly_skin_reviews` 행 수 증가 여부, Upsert/Insert 에러 없음
   - **Qdrant**: `GET http://localhost:6333/collections/kurly_skin` 로 points count 확인
   - **Slack**: 성공/실패 알림 수신 및 메시지 내 통계(신규 제품 수, 신규 리뷰 수) 표시 여부
4. 에러 발생 시: 실패한 노드명, 에러 메시지 확인 후 **문제 해결** 섹션 참고

---

## ⚠️ 주의사항

### 1. 크롤러 실행 시간

- 크롤러 실행 시간은 제품 수에 따라 다릅니다 (수백 개 제품 기준 수십 분 소요)
- 워크플로우 타임아웃 설정 확인 필요

### 2. Postgres 연결

- Postgres 컨테이너가 실행 중이어야 합니다
- Credential 설정이 올바른지 확인하세요

### 3. Ollama 모델

- `bge-m3` 모델이 Ollama에 설치되어 있어야 합니다
- 모델 확인:
  ```bash
  curl http://localhost:11434/api/tags
  ```
- 모델 설치:
  ```bash
  curl http://localhost:11434/api/pull -d '{"name": "bge-m3"}'
  ```

### 4. Qdrant Collection

- `kurly_skin` collection이 생성되어 있어야 합니다
- Collection 벡터 크기는 `bge-m3` 모델의 출력 크기와 일치해야 합니다 (1024차원)

### 5. 임베딩 처리 시간

- 대량의 데이터 처리 시 시간이 오래 걸릴 수 있습니다
- 필요시 배치 크기 조정 또는 병렬 처리 고려

### 6. 에러 핸들링

- 각 노드에 `continueOnFail: true` 설정이 되어 있어 일부 실패해도 계속 진행됩니다
- 실패한 노드는 Slack 알림으로 확인 가능합니다

---

## 🐛 문제 해결

### 크롤러 실행 실패

**증상**: `Execute Crawler` 노드에서 에러 발생

**해결**:
1. 크롤러 파일 경로 확인:
   ```bash
   ls -l /home/julia/workspace/shopping_project/kurly_skin_crawler.py
   ```
2. Python3 설치 확인:
   ```bash
   python3 --version
   ```
3. 의존성 설치 확인:
   ```bash
   pip3 list | grep requests
   ```

### Postgres 연결 실패

**증상**: Postgres 노드에서 연결 에러

**해결**:
1. Postgres 컨테이너 실행 확인:
   ```bash
   docker ps | grep postgres
   ```
2. Credential 설정 확인 (Host, Port, Database, User, Password)
3. 네트워크 연결 확인:
   ```bash
   docker exec -it <postgres_container> psql -U root -d kurly_reviews -c "SELECT 1;"
   ```

### Ollama 임베딩 실패

**증상**: `Generate Embeddings` 노드에서 에러 발생

**해결**:
1. Ollama 컨테이너 실행 확인:
   ```bash
   docker ps | grep ollama
   ```
2. 모델 확인:
   ```bash
   curl http://localhost:11434/api/tags
   ```
3. 모델 설치:
   ```bash
   curl http://localhost:11434/api/pull -d '{"name": "bge-m3"}'
   ```

### Qdrant 저장 실패

**증상**: `Upsert To Qdrant` 노드에서 에러 발생

**해결**:
1. Qdrant 컨테이너 실행 확인:
   ```bash
   docker ps | grep qdrant
   ```
2. Collection 확인:
   ```bash
   curl http://localhost:6333/collections/kurly_skin
   ```
3. 벡터 크기 확인 (bge-m3는 1024차원)

### Slack 알림 실패

**증상**: Slack 메시지가 전송되지 않음

**해결**:
1. Slack Credential 설정 확인
2. Bot Token 확인 (`xoxb-`로 시작)
3. 채널에 Bot 추가 확인:
   ```
   /invite @n8n Kurly Crawler
   ```
4. `SLACK_SETUP_GUIDE.md` 참고

### Slack에서 결과가 전부 0으로 나올 때 (새 제품 0, 새 리뷰 0, 전체 제품 0)

**의미**:
- **"컬리 스킨케어 - 크롤러 실행 실패"** 메시지가 온 경우: 크롤러(crawler-trigger)가 실패했거나 delta를 반환하지 못한 상태입니다. 메시지에 `exitCode` 또는 `stderr` 일부가 포함되므로 원인 추적에 활용하세요.
- **"새 데이터 없음"** 메시지이면서 **전체 제품 수가 0**인 경우: 크롤러가 실패했을 가능성이 큽니다. (정상이면 크롤 성공 시 카테고리 전체 제품 수가 표시됩니다.)

**해결**:
1. crawler-trigger 서버 로그 확인 (크롤러 프로세스 exit code, stderr).
2. n8n 실행 로그에서 `Execute Crawler` 노드 응답 확인 (HTTP 500이면 서버 측 실패).
3. `DATABASE_URL` 등 크롤러 실행 환경 변수 확인.
4. `script/crawler_trigger_server.py`가 크롤러 실패 시에도 `delta`를 빈 구조로 반환하도록 되어 있으므로, 이제 Slack에서는 "크롤러 실행 실패"와 "진짜 새 데이터 없음"이 구분되어 표시됩니다.

### 특정 날짜에 들어간 제품만 Postgres에서 빼고 싶을 때 (권장)

**증상**: 워크플로로 특정 날짜에 **제품만** 들어가고 리뷰는 없었는데, 그날 들어간 제품만 지우고 싶은 경우.

**해결**: 도커 안에서 psql로 삭제하면 됩니다.

```bash
# Postgres 컨테이너 들어가서
docker exec -it <postgres_container> psql -U root -d kurly_reviews -c "DELETE FROM kurly_skin_products WHERE created_at::date = '2026-02-10';"
```

다른 날짜면 `'2026-02-10'`만 바꾸면 됩니다. SQL만 쓰고 싶으면 `script/postgres/remove_products_created_on_date.sql` 참고.

### 특정 delta 파일 기준으로 제거하고 싶을 때

**해결**: 해당 delta JSON 파일에 들어 있던 제품/리뷰만 제거하는 스크립트를 사용할 수 있습니다.

```bash
python script/postgres/remove_delta_from_postgres.py data/kurly_skin_delta_2026-02-10.json --dry-run
python script/postgres/remove_delta_from_postgres.py data/kurly_skin_delta_2026-02-10.json
```

- delta 파일이 비어 있으면 삭제할 항목 0건입니다. 제품만 날짜로 지우려면 위 **특정 날짜에 들어간 제품만** 방식을 쓰는 것이 더 단순합니다.

### 테스트 데이터 전체를 비우고 처음부터 다시 돌리고 싶을 때

**증상**: Postgres·Qdrant를 **전부** 비우고, 크롤링 → 적재 → 임베딩 흐름을 처음부터 다시 확인하고 싶은 경우.

**해결**:

1. **Postgres 전체 비우기** (프로젝트 루트에서):
   ```bash
   bash script/postgres/clear_test_data.sh
   ```
   컨테이너 이름 지정: `bash script/postgres/clear_test_data.sh <postgres_container_name>`

2. **Qdrant 컬렉션 비우기**:
   ```bash
   bash script/qdrant/setup_qdrant_collection.sh
   ```

3. 워크플로 수동 실행 후 크롤링 → Postgres 적재 → 임베딩 → Qdrant까지 처음부터 진행됩니다.

---

## 📊 모니터링

### Postgres 데이터 확인

```sql
-- 전체 제품 수
SELECT COUNT(*) FROM kurly_skin_products;

-- 전체 리뷰 수
SELECT COUNT(*) FROM kurly_skin_reviews;

-- 최근 적재된 제품
SELECT product_no, product_name, created_at 
FROM kurly_skin_products 
ORDER BY created_at DESC 
LIMIT 10;

-- 최근 적재된 리뷰
SELECT id, product_no, LEFT(contents, 50) as preview, registered_at, created_at 
FROM kurly_skin_reviews 
ORDER BY created_at DESC 
LIMIT 10;
```

### Qdrant 데이터 확인

```bash
# Collection 정보
curl http://localhost:6333/collections/kurly_skin

# Point 수 확인
curl http://localhost:6333/collections/kurly_skin/points/scroll \
  -H "Content-Type: application/json" \
  -d '{"limit": 1}'

# 특정 Point 조회
curl http://localhost:6333/collections/kurly_skin/points/1000319181
```

### n8n 실행 로그 확인

1. 워크플로우 편집 화면에서 **"Executions"** 탭 클릭
2. 각 실행의 상세 로그 확인
3. 실패한 노드의 에러 메시지 확인

---

## 🔄 워크플로우 업데이트

워크플로우를 수정한 경우:

1. 워크플로우 편집 화면에서 수정
2. **"Save"** 클릭
3. 변경사항이 자동으로 저장됨

**주의**: Cron 트리거 시간을 변경한 경우, 워크플로우를 **Inactive → Active**로 전환하여 적용하세요.

---

## ✅ 체크리스트

워크플로우 설정 완료 확인:

- [ ] Docker 컨테이너 모두 실행 중 (Postgres, Ollama, Qdrant, n8n)
- [ ] Postgres 스키마 생성 완료 (`kurly_skin_products`, `kurly_skin_reviews`)
- [ ] Qdrant Collection 생성 완료 (`kurly_skin`)
- [ ] 크롤러 코드 존재 및 실행 가능
- [ ] 워크플로우 Import 완료
- [ ] Postgres Credential 설정 완료 (모든 Postgres 노드)
- [ ] Slack Credential 설정 완료 (Slack 노드들)
- [ ] Slack 채널에 Bot 추가 완료
- [ ] 수동 실행 테스트 성공
- [ ] Cron 트리거 시간 확인 (`0 0 * * *` - 매일 12:00 KST)
- [ ] 워크플로우 Active 상태로 전환

---

## 📚 참고 자료

- 크롤러 코드: `kurly_skin_crawler.py`
- 임베딩 스크립트: `script/qdrant/initial_embedding.py`
- Postgres 스키마: `script/postgres/database_setup.sql`
- Slack 설정 가이드: `SLACK_SETUP_GUIDE.md`
- Qdrant 설정: `script/qdrant/setup_qdrant_collection.sh`

---

## 🎉 완료!

이제 매일 오전 12시마다 자동으로:
1. ✅ 컬리 스킨케어 데이터 크롤링
2. ✅ 새로운 데이터만 필터링 (증분 처리)
3. ✅ PostgreSQL에 적재
4. ✅ Ollama로 임베딩 생성
5. ✅ Qdrant에 벡터 저장
6. ✅ Slack으로 결과 알림

**문제가 있으면 언제든지 질문하세요!** 🚀
