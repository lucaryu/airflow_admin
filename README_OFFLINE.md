# 폐쇄망 환경 설치 및 실행 가이드

이 문서는 인터넷 연결이 불가능한 폐쇄망 환경에서 Toy Airflow 서비스를 설치하고 운영하는 방법을 안내합니다.

## 1. 사전 준비 (인터넷 가능 환경)

폐쇄망으로 소스 코드를 옮기기 전에 인터넷이 연결된 PC에서 다음 단계를 진행합니다.

### 1.1 소스 코드 다운로드
프로젝트 소스 코드를 로컬로 가져옵니다.

### 1.2 외부 정적 자원(CDN) 로컬화
제공된 `download_assets.py` 스크립트를 실행하여 외부 라이브러리(Font Awesome, CodeMirror 등)를 미리 내려받습니다.
```bash
python download_assets.py
```
*주의: Google Fonts(Inter)는 라이선스 정책상 직접 제공하지 않으므로, [Google Fonts](https://fonts.google.com/specimen/Inter)에서 다운로드하여 `static/fonts` 폴더에 배치하십시오.*

### 1.3 Python 패키지 의존성 번들링
`requirements.txt`에 명시된 패키지들을 오프라인 설치용 파일(.whl)로 다운로드합니다.
```bash
mkdir wheels
pip download -d ./wheels -r requirements.txt
```

---

## 2. 폐쇄망 환경 설치 및 실행

준비된 전체 폴더(`wheels` 폴더 포함)를 폐쇄망 서버로 복사한 후 다음 과정을 진행합니다.

### 2.1 패키지 오프라인 설치
외부 인터넷을 사용하지 않고 로컬의 `wheels` 폴더를 참조하여 설치합니다.
```bash
pip install --no-index --find-links=./wheels -r requirements.txt
```

### 2.2 애플리케이션 실행
서버를 실행합니다.
```bash
python app.py
```

## 3. 주요 대응 사항 요약
- **CDN 제거**: 모든 HTML 템플릿의 외부 링크를 `static/vendor` 로컬 경로로 전환했습니다.
- **폰트 대응**: 인터넷 연결이 없는 경우 시스템 폰트(Segoe UI, Roboto 등)를 우선 사용하도록 설정했습니다.
- **오프라인 패키지**: `pip download`를 통해 모든 라이브러리를 번들링하여 전달할 수 있습니다.
