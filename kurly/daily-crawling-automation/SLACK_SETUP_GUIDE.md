# 📱 Slack 알림 설정 가이드

n8n 워크플로우에서 Slack 알림을 받기 위한 설정 방법입니다.

---

## 🚀 **1단계: Slack App 생성**

### 💡 **중요: Slack 개발자 페이지로 이동**
⚠️ Slack 워크스페이스 내부의 "앱 → 애플리케이션 추가" 메뉴가 **아닙니다!**  
별도의 개발자 페이지에서 새 앱을 만들어야 합니다.

### 1. Slack API 개발자 페이지 접속
**브라우저 새 탭에서 다음 주소로 이동:**
```
https://api.slack.com/apps
```

### 2. App 생성 시작
- 로그인 후 **"Create New App"** 또는 **"Create an App"** 버튼 클릭
- 또는 **"Your Apps"** 페이지에서 생성 버튼 찾기

### 3. 생성 방식 선택
팝업이 뜨면:
- ✅ **"From scratch"** 선택 (처음부터 만들기)
- ~~"From an app manifest"~~ (템플릿 - 선택 안 함)

### 4. App 정보 입력
- **App Name**: `n8n Kurly Crawler` (또는 원하는 이름)
- **Pick a workspace**: 알림을 받을 워크스페이스 선택
- **"Create App"** 클릭

💰 **무료입니다!** Bot 생성 및 메시지 전송은 완전 무료입니다.

---

## 🔑 **2단계: OAuth & Permissions 설정**

### 1. 좌측 메뉴에서 **"OAuth & Permissions"** 클릭

### 2. Scopes 추가
**Bot Token Scopes** 섹션에서 다음 권한 추가:
- ✅ `chat:write` - 메시지 전송
- ✅ `chat:write.public` - 공개 채널에 메시지 전송
- ✅ `channels:read` - 채널 목록 읽기

### 3. App 설치
- 페이지 상단의 **"Install to Workspace"** 클릭
- **"Allow"** 클릭하여 권한 승인

### 4. Bot Token 복사
- **"Bot User OAuth Token"** 복사 (xoxb-로 시작)
- 이 토큰을 안전하게 보관!

---

## 🔧 **3단계: n8n에서 Slack Credential 생성**

### 1. n8n UI 접속
- http://localhost:5678 접속

### 2. Credentials 추가
1. 우측 상단 **프로필 아이콘** → **Settings** → **Credentials**
2. **"Add Credential"** 클릭
3. **"Slack"** 검색 후 선택
4. Credential 타입 선택:
   - **"Slack OAuth2 API"** 선택
   - 또는 **"Slack API"** 선택

### 3. Token 입력

다음 2개 필드가 보여야 정상입니다:

#### **Access Token** (필수 ⭐)
- 2단계에서 복사한 **Bot User OAuth Token** 붙여넣기
- `xoxb-`로 시작하는 토큰
- ✅ **반드시 입력해야 합니다!**

#### **Signature Secret** (선택 사항)
- Slack에서 n8n으로 **요청이 올 때** 검증용
- 우리는 n8n → Slack 방향으로만 메시지를 보내므로
- ℹ️ **비워두셔도 됩니다!** (입력하지 않아도 작동함)

#### **Name** (필수)
- `Slack account` (또는 원하는 이름)

### 4. 저장
- **"Save"** 클릭

---

### ⚠️ **주의사항**

만약 **Client ID, Client Secret, OAuth Redirect URL** 필드가 보인다면:
- ❌ 잘못된 Credential 타입을 선택한 것입니다
- **뒤로 가기** 후 **"Slack OAuth2 API"** 또는 **"Slack API"** 재선택
- **Access Token 필드만** 있는 화면이 나와야 정상입니다!

---

## 📥 **4단계: 워크플로우 Import**

### 1. 워크플로우 파일 Import
1. n8n UI에서 좌측 상단 **"Workflows"** 메뉴
2. 우측 상단 **"Add workflow"** 드롭다운 → **"Import from file"**
3. 다음 파일 선택:
   ```
   /home/julia/workspace/shopping_project/n8n_workflows/daily_crawl_workflow_with_slack_error_handling.json
   ```
4. **"Import"** 클릭

### 2. Slack 노드 설정 (3개 노드 모두 설정)

워크플로우에는 **3개의 Slack 알림 노드**가 있습니다:
1. **Slack Success Notification** - 성공 시
2. **Slack No Data Notification** - 새 데이터 없을 시
3. **Slack Error Notification** - 실패 시

**각 노드마다 다음 설정을 반복:**
1. 노드 클릭
2. **Credential** 드롭다운에서 `Slack account` 선택
3. **Channel** 선택:
   - 드롭다운에서 알림을 받을 채널 선택 (예: `#general`, `#kurly-crawler`)
4. **"Save"** 클릭

💡 **팁**: 3개 노드 모두 같은 채널로 설정하시면 됩니다!

---

## 🎯 **5단계: 워크플로우 테스트**

### 1. 수동 테스트
1. 워크플로우 편집 화면에서 우측 상단 **"Test workflow"** 클릭
2. **"Daily Cron (00:00)"** 노드를 **Webhook Trigger**로 임시 변경
3. 테스트 실행
4. Slack 채널에 메시지가 도착하는지 확인

### 2. 실제 실행 확인
- 워크플로우를 **Active**로 설정
- 다음날 오전 12시에 자동 실행 대기

---

## 📊 **Slack 알림 메시지 예시**

워크플로우는 상황에 따라 **3가지 다른 메시지**를 보냅니다:

### ✅ **1. 성공 시**
```
🎉 *컬리 스킨케어 데이터 크롤링 완료!*

📅 *날짜:* 2026-02-05

📊 *오늘의 업데이트:*
• 🆕 새로운 제품: *3개*
• 💬 새로운 리뷰: *127개*
• 📦 전체 제품 수: *276개*

✅ 모든 데이터가 성공적으로 PostgreSQL에 적재되었습니다!
```

### ℹ️ **2. 새 데이터 없을 시**
```
ℹ️ *컬리 스킨케어 데이터 크롤링 완료*

📅 *날짜:* 2026-02-05

📊 *결과:*
• 🆕 새로운 제품: *0개*
• 💬 새로운 리뷰: *0개*
• 📦 전체 제품 수: *276개*

✅ 새로운 데이터가 없습니다. 모든 데이터가 최신 상태입니다!
```

### ❌ **3. 실패 시**
```
❌ *컬리 스킨케어 크롤링 실패!*

📅 *날짜:* 2026-02-05
⏰ *시간:* 00:15:32

🚨 *오류 유형:* 크롤러 실행 실패
📍 *오류 발생 위치:* Execute Crawler

💥 *오류 메시지:*
```
Command failed with exit code 1
```

📝 *상세 정보:*
Python module not found

⚠️ 워크플로우를 확인하고 다시 실행해주세요!
```

---

## 🛠️ **문제 해결**

### ❌ "channel_not_found" 오류
- Slack App을 해당 채널에 추가:
  1. 채널 열기
  2. `/invite @n8n Kurly Crawler` 입력

### ❌ "not_in_channel" 오류
- Bot이 채널에 초대되지 않음
- `/invite @n8n Kurly Crawler` 실행

### ❌ "invalid_auth" 오류
- Bot Token이 잘못됨
- Slack API 페이지에서 토큰 재확인

### ❌ Channel "From list"에서 리스트가 안 나와요
채널 선택 시 드롭다운 리스트가 비어 있거나 로딩만 될 때:

1. **Scope 추가 후 재설치**
   - https://api.slack.com/apps → 해당 앱 → **OAuth & Permissions**
   - **Bot Token Scopes**에 `channels:read`가 있는지 확인
   - **한 번이라도 Scope를 수정했다면** 상단 **"Reinstall to Workspace"** 클릭 후 **Allow** 다시 하기 (재설치해야 새 권한이 적용됨)

2. **봇을 채널에 먼저 초대**
   - Slack에서 알림 받을 채널로 이동
   - 메시지 입력창에 `/invite @앱이름` (예: `@n8n Kurly Crawler`) 입력
   - 일부 버전에서는 **봇이 멤버인 채널만** 리스트에 나올 수 있음

3. **리스트 대신 채널 이름 직접 입력 (권장)**
   - n8n Slack 노드에서 **Channel** 설정 시
   - "From list" 대신 **"By name"** (또는 "채널 이름으로 선택") 선택
   - 채널 이름만 입력 (예: `general` 또는 `#general`)
   - 리스트가 안 나와도 이렇게 하면 동작함

---

## 🎨 **커스터마이징**

### 메시지 내용 수정
워크플로우의 **"Slack Notification"** 노드에서 `text` 필드를 수정하여 메시지 내용을 변경할 수 있습니다.

### 추가 정보 포함
- 평균 리뷰 점수
- 최고/최저 평점 제품
- 에러 발생 여부

---

## ✅ **완료 체크리스트**

설정이 완료되었는지 확인하세요:

- [ ] Slack App 생성 완료 (https://api.slack.com/apps)
- [ ] Bot Token Scopes 3개 추가 완료 (`chat:write`, `chat:write.public`, `channels:read`)
- [ ] 워크스페이스에 App 설치 완료
- [ ] Bot Token 복사 완료 (`xoxb-...`)
- [ ] n8n Slack Credential 생성 완료
- [ ] 워크플로우 Import 완료
- [ ] **3개의 Slack 노드** 모두 Credential 설정 완료
- [ ] **3개의 Slack 노드** 모두 Channel 선택 완료
- [ ] 워크플로우 Active 상태로 전환
- [ ] 수동 테스트 성공
- [ ] Slack 채널에서 알림 수신 확인

---

## 💡 **자주 묻는 질문 (FAQ)**

### Q1: Slack 워크스페이스 내부에서 "Create New App" 버튼을 못 찾겠어요!
**A**: Slack 워크스페이스가 아니라 **https://api.slack.com/apps** 개발자 페이지로 가셔야 합니다!

### Q2: n8n에서 Client ID, Client Secret을 입력하라고 해요!
**A**: 잘못된 Credential 타입을 선택하셨습니다. "Slack OAuth2 API"를 선택하고 **Access Token 필드만** 있는 화면이 나와야 합니다.

### Q3: Signature Secret은 반드시 입력해야 하나요?
**A**: 아니요! 우리는 n8n에서 Slack으로 메시지만 보내므로 **비워두셔도 됩니다**.

### Q4: 비용이 발생하나요?
**A**: 완전 무료입니다! Slack Bot 생성 및 메시지 전송은 무료입니다.

### Q5: 워크플로우가 3개의 Slack 노드가 있는데 모두 설정해야 하나요?
**A**: 네! Success, No Data, Error 3가지 상황에 대한 알림이므로 모두 설정해주세요.

---

## 🎉 **완료!**

이제 매일 오전 12시마다 자동으로:
1. ✅ 컬리 스킨케어 데이터 크롤링
2. ✅ 새로운 데이터만 필터링
3. ✅ Ollama로 감성 분석
4. ✅ PostgreSQL에 저장
5. ✅ **Slack으로 결과 알림** (성공/새 데이터 없음/실패)

**문제가 있으면 언제든지 질문하세요!** 🚀
