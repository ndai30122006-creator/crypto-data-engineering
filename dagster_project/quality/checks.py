"""Pure data-quality checks (không IO, không Dagster — test offline được).

Mỗi check trả về CheckResult(passed, message, metrics). Metrics là số liệu
thô để asset check đẩy lên UI + log (mục pipeline metrics).
"""
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class CheckResult:
    """Kết quả 1 check: đạt/không + message + metrics cho UI/log."""

    passed: bool
    message: str
    metrics: dict = field(default_factory=dict)


def _as_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def check_freshness(
    records: list[dict],
    ts_field: str = "published_at",
    max_age_minutes: float = 24 * 60,
    now: datetime | None = None,
) -> CheckResult:
    """11. Dữ liệu có mới không: bản tin dated mới nhất phải trong max_age."""
    now = _as_utc(now or datetime.now(UTC))
    stamps: list[datetime] = []
    for r in records:
        raw = r.get(ts_field)  # datetime (trong asset) hoặc ISO str (sau dump json)
        if isinstance(raw, datetime):
            stamps.append(_as_utc(raw))
        elif isinstance(raw, str):
            try:
                stamps.append(_as_utc(datetime.fromisoformat(raw)))
            except ValueError:
                continue
    if not stamps:
        return CheckResult(False, f"no parseable {ts_field} in {len(records)} records",
                           {"records": len(records), "dated": 0})
    newest = max(stamps)
    age_min = (now - newest).total_seconds() / 60
    return CheckResult(
        age_min <= max_age_minutes,
        f"newest {ts_field} age {age_min:.1f}min (limit {max_age_minutes:.0f}min)",
        {"records": len(records), "dated": len(stamps), "newest_age_min": round(age_min, 1)},
    )


def check_duplicates(records: list[dict], key: str = "url") -> CheckResult:
    """12. Không trùng key (dedupe ở cleaner phải đảm bảo điều này)."""
    seen: set = set()
    dupes = 0
    for r in records:
        v = r.get(key)
        if v in seen:
            dupes += 1
        seen.add(v)
    return CheckResult(
        dupes == 0,
        f"{dupes} duplicate {key} in {len(records)} records",
        {"records": len(records), "duplicates": dupes},
    )


def check_nulls(records: list[dict], required: list[str]) -> CheckResult:
    """13. Field bắt buộc không null/rỗng."""
    bad = sum(
        1 for r in records
        if any(r.get(f) is None or r.get(f) == "" for f in required)
    )
    return CheckResult(
        bad == 0,
        f"{bad}/{len(records)} records missing {required}",
        {"records": len(records), "null_violations": bad},
    )


def check_row_count(
    count: int, min_count: int = 1, max_count: int | None = None
) -> CheckResult:
    """14. Row-count bất thường: feed rỗng (chết nguồn) hoặc bùng nổ (lỗi parse)."""
    if count < min_count:
        return CheckResult(False, f"row count {count} < min {min_count} (source chết?)",
                           {"count": count})
    if max_count is not None and count > max_count:
        return CheckResult(False, f"row count {count} > max {max_count} (parse lặp?)",
                           {"count": count})
    return CheckResult(True, f"row count {count} trong ngưỡng", {"count": count})


def check_schema(records: list[dict], model) -> CheckResult:
    """15. Mọi record parse được qua Pydantic model (không ValidationError)."""
    bad = 0
    first_error = ""
    for r in records:
        try:
            model(**r)
        except Exception as exc:  # noqa: BLE001 - gom mọi lỗi schema
            bad += 1
            if not first_error:
                first_error = str(exc)[:200]
    msg = f"{bad}/{len(records)} schema violations" + (f": {first_error}" if bad else "")
    return CheckResult(bad == 0, msg, {"records": len(records), "violations": bad})


def summarize(pipeline: str, results: dict[str, CheckResult]) -> dict:
    """16. Gom metrics các check thành 1 dict cho asset metadata/log."""
    return {
        "pipeline": pipeline,
        "passed": sum(1 for r in results.values() if r.passed),
        "failed": sum(1 for r in results.values() if not r.passed),
        **{f"{name}_{k}": v for name, r in results.items() for k, v in r.metrics.items()},
    }
