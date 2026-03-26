import argparse
import sys
from typing import Optional

from app.batch.aggregation import run_daily_kpi_batch
from app.batch.date_range import parse_inclusive_date_range


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        start_date, end_date = parse_inclusive_date_range(
            args.start_date,
            args.end_date,
        )
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    run_daily_kpi_batch(start_date=start_date, end_date=end_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
