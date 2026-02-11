#!/bin/bash
# Qdrant 컬렉션 생성 스크립트

set -e

echo "🚀 Qdrant 컬렉션 생성 시작..."
echo ""

# 1. Qdrant 상태 확인
echo "📡 Qdrant 서버 확인 중..."
if ! curl -s http://localhost:6333/healthz > /dev/null; then
    echo "❌ Qdrant 서버에 연결할 수 없습니다."
    echo "   Docker 컨테이너를 확인하세요:"
    echo "   docker ps | grep qdrant"
    exit 1
fi
echo "✅ Qdrant 서버 정상"
echo ""

# 2. 기존 컬렉션 삭제 (선택)
echo "🗑️  기존 'kurly_skin' 컬렉션 삭제 중..."
curl -X DELETE "http://localhost:6333/collections/kurly_skin" \
  -H "Content-Type: application/json" 2>/dev/null || true
echo ""
sleep 1

# 3. 새 컬렉션 생성
echo "📦 'kurly_skin' 컬렉션 생성 중..."
curl -X PUT "http://localhost:6333/collections/kurly_skin" \
  -H "Content-Type: application/json" \
  -d '{
    "vectors": {
      "size": 1024,
      "distance": "Cosine"
    }
  }'
echo ""
echo ""

echo "✅ Qdrant 'kurly_skin' 컬렉션 생성 완료!"
