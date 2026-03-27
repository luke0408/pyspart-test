# 이커머스 KPI 데모를 위한 Spark 테스트 환경

이 프로젝트는 FastAPI, PostgreSQL, PySpark를 사용하여 로컬에서 엔드투엔드 KPI 파이프라인을 시연합니다.

## 사전 요구 사항

- Docker 및 Docker Compose
- Python 3.11+ (로컬 스크립트 실행용)

## 로컬 런북

상세한 시스템 구조와 데이터 흐름은 [docs/structure.md](docs/structure.md)를 참조하십시오.
실행/검증 단계의 시스템 관점 요약은 `docs/structure.md`의 `## 실행 흐름`, `## 검증 흐름` 섹션에서 확인할 수 있습니다.

다음 명령어를 순서대로 실행하여 환경을 부팅하고, 데이터를 시드하고, 배치 집계를 실행한 후 결과를 검증하십시오.

### 0. 환경 파일 준비
runtime 구성의 단일 소스로 `.env`를 사용합니다.
```bash
cp .env.example .env
```

### 1. 환경 부팅
PostgreSQL, API, Spark, Grafana 서비스를 시작합니다.
```bash
docker compose up -d --build
```

### 2. 데이터베이스 마이그레이션 실행
PostgreSQL 데이터베이스에 스키마를 적용합니다.
```bash
docker compose exec api alembic upgrade head
```

### 3. Grafana 읽기 전용 데이터베이스 역할 초기화
KPI 요약 테이블에 대한 읽기 전용 권한을 가진 Grafana 데이터소스 역할을 생성하거나 업데이트합니다.
```bash
docker compose exec api python scripts/bootstrap_grafana_readonly_role.py
```

### 4. 결정론적 원시 데이터 시딩
샘플 이커머스 이벤트를 원시 테이블에 삽입합니다.
```bash
python3 scripts/seed_data.py
```

### 5. Spark 배치 집계 실행
`2026-03-01`부터 `2026-03-03`까지(포함)의 일일 KPI를 계산합니다.
```bash
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-03
```

### 6. SQL 요약 테이블 검증
데이터베이스에서 직접 집계 결과를 확인합니다.
```bash
docker compose exec postgres psql -U postgres -c "SELECT * FROM daily_traffic_summary ORDER BY summary_date ASC;"
docker compose exec postgres psql -U postgres -c "SELECT summary_date, view_users, cart_users, order_users, payment_users FROM daily_conversion_funnel ORDER BY summary_date ASC;"
```

### 7. API 엔드포인트 검증
FastAPI 읽기 엔드포인트를 통해 KPI 결과를 확인합니다.
```bash
# Daily Traffic
curl -s "http://localhost:8000/kpi/traffic/daily?summary_date=2026-03-01" | jq .

# Ranged Funnel
curl -s "http://localhost:8000/kpi/funnel/range?start_date=2026-03-01&end_date=2026-03-03" | jq .
```

### 8. Grafana 프로비저닝 및 대시보드 검증
Grafana 상태를 확인하고 결정론적 프로비저닝/데이터 검사를 실행합니다.
```bash
curl -s "http://localhost:3000/api/health" | jq .
python3 scripts/verify_grafana_dashboard.py
```

### 9. 배치 재실행 (멱등성 확인)
동일한 범위에 대해 배치를 재실행하면 기존 행이 동일한 값으로 교체되어야 합니다.
```bash
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-03
```

### 10. 환경 정리
모든 서비스를 중지하고 볼륨을 제거합니다.
```bash
docker compose down -v
```

## 자동 검증

자동 검증 스위트를 실행합니다:
```bash
FORCE_SPARK_CONTAINER=true python3 -m pytest -q
python3 scripts/verify_grafana_dashboard.py
```

`scripts/verify_grafana_dashboard.py`는 Docker 서비스가 실행 중이고 위의 런북을 통해 데이터가 시딩되었다고 가정합니다.

`jq`는 선택 사항입니다. 설치되어 있지 않은 경우 curl 예제에서 `| jq .`를 제거하십시오.
