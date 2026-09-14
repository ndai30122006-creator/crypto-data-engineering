"""Logger JSON thống nhất cho services (jlogger, fallback stdlib).

- Có jlogger (Linux image + dev Windows đều cài được): log JSON structured
  ra stderr, API fluent `info(msg, **fields)`.
- Không có (import lỗi): fallback stdlib logging, cùng cú pháp gọi để
  service/test không phải rẽ nhánh.
- Dagster assets/resources giữ `context.log` / `get_dagster_logger`
  (hệ log riêng của Dagster) — không lẫn với hệ này.
"""
import logging

try:
    import jlogger as _jlogger

    _HAS_JLOGGER = True
except ImportError:  # pragma: no cover - môi trường thiếu dep
    _jlogger = None
    _HAS_JLOGGER = False


class JLog:
    """Adapter mỏng: cùng cú pháp, backend jlogger hoặc stdlib."""

    def __init__(self, name: str, backend: str = "auto", _base=None):
        if backend == "auto":
            backend = "jlogger" if _HAS_JLOGGER else "stdlib"
        self._backend = backend
        if backend == "jlogger":
            self._log = _base if _base is not None else _jlogger.getLogger(name)
        else:
            self._log = _base if _base is not None else logging.getLogger(name)
        self._name = name

    def with_fields(self, **fields) -> "JLog":
        """Logger con gắn sẵn context (vd symbol) — base bất biến."""
        if self._backend == "jlogger":
            return JLog(self._name, "jlogger", self._log.with_values(**fields))
        child = JLog(self._name, "stdlib")
        child._fields = {**getattr(self, "_fields", {}), **fields}
        return child

    def _emit(self, level: str, msg: str, exc=None, **fields) -> None:
        if self._backend == "jlogger":
            event = getattr(self._log, level)()
            if exc is not None:
                event = event.with_error(exc)
            if fields:
                event = event.with_values(**fields)
            event.msg(msg)
            return
        merged = {**getattr(self, "_fields", {}), **fields}
        if exc is not None:
            merged["error"] = str(exc)
        suffix = f" {merged}" if merged else ""
        getattr(self._log, level)(f"{msg}{suffix}")

    def debug(self, msg: str, **fields) -> None:
        self._emit("debug", msg, **fields)

    def info(self, msg: str, **fields) -> None:
        self._emit("info", msg, **fields)

    def warning(self, msg: str, **fields) -> None:
        self._emit("warning", msg, **fields)

    def error(self, msg: str, exc=None, **fields) -> None:
        self._emit("error", msg, exc=exc, **fields)


def get_logger(name: str) -> JLog:
    return JLog(name)
