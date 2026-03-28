# 2026년 3월 더미 데이터 시나리오 분석

이 문서는 `scripts/seed_data.py`에서 생성하는 2026년 3월 더미 데이터의 비즈니스 가정과 KPI 형태를 설명합니다.

## 시나리오 목표

- 월간 활성 사용자(MAU) 약 **1,000명** 규모를 가정합니다.
- **3월 16일 이벤트(선착순 할인 쿠폰)**로 트래픽과 결제 도달율이 동시 상승하는 패턴을 만듭니다.
- 이벤트 이후에는 약 **600명 전후 리텐션**을 보이다가, 월말로 갈수록 점진 하락하는 흐름을 반영합니다.

## 핵심 가정

- 분석 기간: `2026-03-01` ~ `2026-03-31`
- 월간 사용자 풀: `1,000`명
- 이벤트 피크 날짜: `2026-03-16`
- 이벤트 이전 DAU 기준선: 약 `400`
- 이벤트 당일 DAU 피크: `800`
- 이벤트 이후 DAU: 약 `600`에서 시작해 월말 `460` 내외로 하락

## KPI 패턴 요약

아래 값은 시나리오가 의도대로 동작하는지 확인하기 위한 대표 날짜 스냅샷입니다.

| 날짜 | DAU | View | Cart | Order | Payment(Completed) | Payment/View |
|---|---:|---:|---:|---:|---:|---:|
| 2026-03-01 | 402 | 402 | 141 | 72 | 51 | 0.1269 |
| 2026-03-15 | 392 | 392 | 145 | 72 | 51 | 0.1301 |
| 2026-03-16 (이벤트) | 800 | 800 | 416 | 275 | 239 | 0.2988 |
| 2026-03-17 | 592 | 592 | 260 | 156 | 125 | 0.2111 |
| 2026-03-24 | 531 | 531 | 212 | 118 | 88 | 0.1657 |
| 2026-03-31 | 460 | 460 | 166 | 85 | 60 | 0.1304 |

해석:

- 이벤트 전(3/1~3/15): DAU가 약 400대에서 안정적으로 유지됩니다.
- 이벤트 당일(3/16): DAU 800 피크와 함께 결제 도달율(`payment_from_view_rate`)이 월중 최고치로 상승합니다.
- 이벤트 후(3/17~3/31): 초반 600명대 리텐션 이후 점진 하락 곡선을 보입니다.

## 대시보드 데이터 구성

요청된 네 가지 데이터 묶음은 `scripts/seed_data.py`의 `calculate_dashboard_summaries()`에서 제공합니다.

- `traffic_summary`: 일자별 `dau_users`
- `funnel_summary`: 일자별 `view_users`, `cart_users`, `order_users`, `payment_users`
- `aggregation_rate`: 일자별 전환율
  - `cart_from_view_rate`
  - `order_from_cart_rate`
  - `payment_from_order_rate`
  - `payment_from_view_rate`
- `aggregation_coverage`:
  - `aggregation_range_start`, `aggregation_range_end`
  - `covered_days`, `expected_days`, `coverage_rate`
  - `monthly_active_users`
  - `peak_summary_date`, `peak_dau_users`

## 확인 방법

```bash
# 시나리오 기반 원천 더미 데이터 적재
python3 scripts/seed_data.py

# 3월 전체 집계 실행
FORCE_SPARK_CONTAINER=true python3 scripts/run_batch.py --start-date 2026-03-01 --end-date 2026-03-31

# 시나리오 요약(traffic/funnel/rate/coverage) 확인
python3 scripts/seed_data.py --print-dashboard-summaries
```

## 관련 파일

- 시나리오 생성 로직: `scripts/seed_data.py`
- 시스템 구조 문서: `docs/structure.md`
- 실행 런북: `README.md`
