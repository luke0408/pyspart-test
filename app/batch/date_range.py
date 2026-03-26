from datetime import date, timedelta

from app.core.timezone import parse_iso_date


def parse_inclusive_date_range(start_date: str, end_date: str) -> tuple[date, date]:
    parsed_start_date = parse_iso_date(start_date)
    parsed_end_date = parse_iso_date(end_date)
    if parsed_start_date > parsed_end_date:
        raise ValueError("start_date must be <= end_date")
    return parsed_start_date, parsed_end_date


def enumerate_dates(start_date: date, end_date: date) -> list[date]:
    day_count = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(day_count + 1)]
