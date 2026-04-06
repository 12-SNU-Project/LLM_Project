# 📊 SNU12 Team07 Project

<div align="center">

![Project Status](https://img.shields.io/badge/status-in%20progress-yellow)

**LLM 처리 기반 RAG 시스템 구현**

</div>

## 프로젝트 소개

본 프로젝트는 삼성전자의 2014–2024년 감사보고서 HTML 파일을 활용하여 금융 도메인 특화 NLP 시스템을 구축한다.
HTML 파싱과 기본적인 데이터 파이프라인 구성을 토대로, CI/CD 환경을 갖춘 **완성도 있는 최종 응용 시스템**을 개발하는 것이 목표이다.

---

## 🛠 기술 스택
### 
### 사용 언어
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
### 패키지
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-44A833?style=for-the-badge&logo=html5&logoColor=white)
![Hugging Face](https://img.shields.io/badge/Hugging%20Face-8A2BE2?style=for-the-badge&logo=huggingface&logoColor=white)
![LangChain](https://img.shields.io/badge/%20LangChain-1C3C3C?style=for-the-badge&logo=langchain&logoColor=white)
### Tools
![Conda](https://img.shields.io/badge/Anaconda-44A833?style=for-the-badge&logo=anaconda&logoColor=white)
![ChromaDB](https://img.shields.io/badge/ChromaDB-004A7C?style=for-the-badge&logo=google-cloud&logoColor=white) 
![Ollama](https://img.shields.io/badge/ollama-000000?style=for-the-badge&logo=ollama&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
---

## 📁 프로젝트 구조 (업데이트)
```
📦 root
│
├── 📂 src/                       # 소스코드
│   ├── 📂 ohsd_kimsb/   
│   ├── 📂 kimsb/   
│   ├── 📂 leemh_ohjy/
│   └── 📂 parkhs_hajm/
├── 📂 data/                      # 감사 보고서 문서
│
├──  과제문서.ipynb
├── .gitignore
└──  README.md
```
---

## 팀원
| 이름 | 역할 | GitHub |
|--------|------|--------|
| 오승담 | 개발 | [@seungdam](https://github.com/seungdam) |
| 하재민 | 개발 | [@JMJM-create](https://github.com/JMJM-create) |
| 박현서 | 개발 | [@hyeonseo021003-ops](https://github.com/hyeonseo021003-ops) |
| 이민환 | 개발 | [@q277wsvtzt](https://github.com/q277wsvtzt) |
| 오주영 | 개발 | [@dhwn1323-a11y](https://github.com/dhwn1323-a11y) |
| 김수비 | 개발 | [@ksubi0403](https://github.com/ksubi0403) |
---


## 📊 주요 파이프라인
**html 파싱 및 DB 구축**
   - 테이블 / 주석 / 맥락을 고려한 파싱
   - 주요 핵심 표에 대한 RDB(Relational Database 구축)
   - VDB을 활용해 사용자 입력에 대한 컨텍스트 추출
---

## 📝 데이터 출처

**SNU 빅데이터 ai 핀테크 고급 전문가 과정에서 제공**

---
### 커밋 컨벤션
- `feat`: 새로운 기능 추가
- `fix`: 버그 수정
- `docs`: 문서 수정
- `style`: 코드 포맷팅, 세미콜론 누락 등
- `refactor`: 코드 리팩토링
- `test`: 테스트 코드
- `chore`: 빌드, 프로젝트 설정 등
---
