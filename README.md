# data-stack-playground

개인 데이터 엔지니어링 작업물을 모아두는 공간입니다.

## 현재 프로젝트

### Docker Data Stack
- 초기 데이터 적재 자동화 파이프라인
- 매일 증분 데이터 적재 자동화 파이프라인
- PostgreSQL + Qdrant 데이터 적재 파이프라인
- Docker Compose 기반 환경 구성

## 사용 방법
```bash
# 저장소 클론
git clone https://github.com/[username]/data-stack-playground.git

# 각 프로젝트 폴더의 README 참고
```

## 구조
```
.
├── docker-postgres-qdrant/   # PostgreSQL + Qdrant 데이터 파이프라인
├── initial
├── daliy
└── ...                        # 추후 추가될 프로젝트들
```

## Tech Stack
- Docker & Docker Compose
- PostgreSQL
- Qdrant
- n8n
