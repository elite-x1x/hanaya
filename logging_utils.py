import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

class CompactFormatter(logging.Formatter):
    """Formatter ringkas dengan warna terminal"""
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[35m',
        'RESET': '\033[0m'
    }

    def format(self, record):
        # Format timestamp dengan milidetik
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        timestamp = f"{timestamp},{int(record.msecs):03d}"
        level = record.levelname
        message = record.getMessage()
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        return f"{timestamp} [{color}{level}{reset}] {message}"

class NetworkErrorFilter(logging.Filter):
    """Filter untuk mengurangi spam error berulang"""
    def __init__(self, max_same_errors=5):
        super().__init__()
        self.error_cache = {}
        self.max_same_errors = max_same_errors

    def filter(self, record):
        if record.levelno >= logging.ERROR and record.exc_info:
            exc_type, _, _ = record.exc_info
            exc_name = exc_type.__name__ if exc_type else "Unknown"
            count, last_time = self.error_cache.get(exc_name, (0, 0))
            now = datetime.now(timezone.utc).timestamp()
            if now - last_time > 60:
                self.error_cache[exc_name] = (1, now)
            else:
                self.error_cache[exc_name] = (count + 1, now)
                if count >= self.max_same_errors:
                    return False
        return True

def setup_logging():
    """Setup logging global dengan file + stream handler"""
    formatter = CompactFormatter()
    file_handler = RotatingFileHandler("bot.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # Tambahkan filter untuk kurangi spam error
    network_filter = NetworkErrorFilter(max_same_errors=3)
    stream_handler.addFilter(network_filter)
    file_handler.addFilter(network_filter)

    logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
