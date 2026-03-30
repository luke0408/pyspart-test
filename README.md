# 이커머스 KPI 데모를 위한 Spark 테스트 환경

이 프로젝트는 FastAPI, PostgreSQL, PySpark를 사용하여 로컬에서 엔드투엔드 KPI 파이프라인을 시연합니다.

## 사전 요구 사항

- Docker 및 Docker Compose
- Python 3.11+ (로컬 스크립트 실행용)

## OS별 실행 런북 (macOS / Windows / Linux)

아래는 OS별로 달라지는 실행 포인트만 빠르게 정리한 섹션입니다. 핵심 순서는 동일합니다.

| 항목 | macOS / Linux | Windows (PowerShell) |
| --- | --- | --- |
| `.env` 준비 | `cp .env.example .env` | `Copy-Item .env.example .env` |
| 파이썬 실행기 | `python3` | `py -3` (또는 `python`) |
| 일회성 환경변수 | `FORCE_SPARK_CONTAINER=true <command>` | `$env:FORCE_SPARK_CONTAINER='true'; <command>` |
| 환경변수 해제 | `unset FORCE_SPARK_CONTAINER` | `Remove-Item Env:FORCE_SPARK_CONTAINER` |

### macOS 런북 (zsh/bash)

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec api alembic upgrade head
python3 scripts/seed_data.py
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31
docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
curl -s "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01" | jq .
FORCE_SPARK_CONTAINER=true python3 -m pytest -q
docker compose down -v
```

### Linux 런북 (bash)

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec api alembic upgrade head
python3 scripts/seed_data.py
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31
docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
curl -s "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01" | jq .
FORCE_SPARK_CONTAINER=true python3 -m pytest -q
docker compose down -v
```

### Windows 런북 (PowerShell)

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose exec api alembic upgrade head
py -3 scripts/seed_data.py

$env:FORCE_SPARK_CONTAINER='true'
py -3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31
py -3 -m pytest -q
Remove-Item Env:FORCE_SPARK_CONTAINER

docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
Invoke-RestMethod "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01"

docker compose down -v
```

> Windows에서 `py -3`가 없으면 `python`으로 같은 명령을 실행하면 됩니다.
> `jq`가 없으면 `curl ... | jq .` 대신 `curl` 또는 `Invoke-RestMethod`만 사용하세요.

## 로컬 런북

상세한 시스템 구조와 데이터 흐름은 [docs/structure.md](docs/structure.md)를 참조하십시오.
3월 더미 데이터 시나리오(이벤트 피크/리텐션 가정)는 [docs/scenario-analysis.md](docs/scenario-analysis.md)에 정리되어 있습니다.
실행/검증 단계의 시스템 관점 요약은 `docs/structure.md`의 `## 실행 흐름`, `## 검증 흐름` 섹션에서 확인할 수 있습니다.

다음 명령어를 순서대로 실행하여 환경을 부팅하고, 데이터를 시드하고, 배치 집계를 실행한 후 결과를 검증하십시오.

### 0. 환경 파일 준비
runtime 구성의 단일 소스로 `.env`를 사용합니다.
```bash
cp .env.example .env
```

### 1. 환경 부팅
PostgreSQL, API, Spark 서비스를 시작합니다.
```bash
docker compose up -d --build
```

### 2. 데이터베이스 마이그레이션 실행
PostgreSQL 데이터베이스에 스키마를 적용합니다.
```bash
docker compose exec api alembic upgrade head
```

### 3. 결정론적 원시 데이터 시딩
샘플 이커머스 이벤트를 원시 테이블에 삽입합니다.
```bash
python3 scripts/seed_data.py
```

### 4. Spark 배치 집계 실행
`2026-03-01`부터 `2026-03-31`까지(포함)의 일일 KPI를 계산합니다.
```bash
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31
```

### 5. SQL 요약 테이블 검증
데이터베이스에서 직접 집계 결과를 확인합니다.
```bash
docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
docker compose exec postgres psql -U postgres -c "SELECT summary_date, view_users, cart_users, order_users, payment_users FROM daily_conversion_funnel ORDER BY summary_date ASC;"
```

### 6. API 엔드포인트 검증
FastAPI 읽기 엔드포인트를 통해 KPI 결과를 확인합니다.
```bash
# Daily Traffic
curl -s "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01" | jq .

# Ranged Funnel
curl -s "http://localhost:8000/kpi/funnel/range?start_date=2026-03-01&end_date=2026-03-31" | jq .
```

### 7. 배치 재실행 (멱등성 확인)
동일한 범위에 대해 배치를 재실행하면 기존 행이 동일한 값으로 교체되어야 합니다.
```bash
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31
```

### 8. 환경 정리
모든 서비스를 중지하고 볼륨을 제거합니다.
```bash
docker compose down -v
```

## 자동 검증

자동 검증 스위트를 실행합니다:
```bash
FORCE_SPARK_CONTAINER=true python3 -m pytest -q
```

`jq`는 선택 사항입니다. 설치되어 있지 않은 경우 curl 예제에서 `| jq .`를 제거하십시오.
