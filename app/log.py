import json
import logging


class JsonFormatter(logging.Formatter):
    """Renders each log record as a single JSON line. Anything passed via
    logging's extra={...} is merged into the output so we get structured logs
    without pulling in a logging library."""

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, object] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                out[key] = value
        if record.exc_info:
            out["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(out)


_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
