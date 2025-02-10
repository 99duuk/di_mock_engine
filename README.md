# 개발환경 설정
## 1. venv 설치 및 실행
### 1) venv 설치
- `python`이 설치되어 있는지 확인
```bash
python --version
```
- `venv` 생성
프로젝트 디렉터리로 이동한 후 venv 생성
```bash
python -m venv venv
```

### 2) 가상 환경 활성화
- windows
```cmd
.\venv\Scripts\activate
```
- mac or linux
```bash
source venv/bin/activate
```
활성화되면 프롬프트에 `(venv)`가 표시된다.


### 3) 필수 패키지 설치
- `requirements.txt` 파일에 있는 패키지 설치 - 아래 명령어 실행행
```bash
pip install -r requirements.txt
```

### 4) 가상환경 비활성화
- 종료 명령어
```bash
deactivate
```

### 5) kafka - six package가 없다는 에러가 나오는 경우
1. 
해당 에러는 파이썬 버전과 카프카 라이브러리의 충돌로 추정됨...
파이썬 버전을 3.11이하로 낮춰야함.
파이썬 3.11 다운로드 후 아래 명령어로 3.11이 설치되어 있는지 확인
```cmd
py --list
py -3.11 --version
```

3.11로 가상환경 생성
```cmd
py -3.11 -m venv venv
```
생성 후 가상환경 활성화

2. 
혹은 다음과 같이 kafka-pyhon을 재설치해 해결할 수도 있다.
```cmd
 pip install --break-system-packages git+https://github.com/dpkp/kafka-python.git
```

### 6) face_recognition 설치 에러
cmake를 설치해야 함. 



