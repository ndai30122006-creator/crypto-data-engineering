"""Rules kiểm tra + tách valid/errors (bad-record pattern)."""


def validate_record(record: dict) -> str | None:
    """Trả về None nếu OK, ngược lại trả về lý do lỗi."""
    if not record.get("symbol"):
        return "missing symbol"
    price = record.get("price")
    if price is None:
        return "missing price"
    if price <= 0:
        return f"price <= 0 ({price})"
    market_cap = record.get("market_cap")
    if market_cap is not None and market_cap <= 0:
        return f"market_cap <= 0 ({market_cap})"
    return None


def validate(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Tách (valid, errors). Error adh dạng đã sẵn sàng INSERT."""
    valid: list[dict] = []
    errors: list[dict] = []
    for record in records:
        reason = validate_record(record)
        if reason is None:
            valid.append(record)
        else:
            errors.append(
                {"pipeline": "market", "payload": record, "error": reason}
            )
    return valid, errors
