import logging
import json
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import traceback
import sys

# ============================================================
# === ENHANCED LOGGING SYSTEM ===
# ============================================================
class CompactFormatter(logging.Formatter):
    """Formatter yang ringkas, hanya tampilkan error essentials tanpa full traceback"""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        if self._is_network_error(record):
            return self._format_network_error(record)
        
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level = record.levelname
        message = record.getMessage()
        
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        if record.exc_info and record.levelno >= logging.ERROR:
            exc_type, exc_value, _ = record.exc_info
            exc_name = exc_type.__name__ if isinstance(exc_type, tuple) else (exc_type.__name__ if exc_type else "Unknown")
            return f"{timestamp} [{color}{level}{reset}] {message}\n    └─ {exc_name}: {str(exc_value)[:150]}"
        
        return f"{timestamp} [{color}{level}{reset}] {message}"
    
    @staticmethod
    def _is_network_error(record):
        network_errors = [
            'httpx.ReadError',
            'httpx.ConnectError',
            'httpx.TimeoutException',
            'ConnectionError',
            'TimeoutError',
            'OSError',
            'socket.error'
        ]

        if record.exc_info:
            exc_type = record.exc_info
            exc_name = exc_type.__name__ if exc_type else ""
            
            return any(err in exc_name for err in network_errors)
        
        return any(err in record.getMessage() for err in network_errors)
    
    @staticmethod
    def _format_network_error(record):
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        level = record.levelname
        message = record.getMessage()
        color = CompactFormatter.COLORS.get(level, '')
        reset = CompactFormatter.COLORS['RESET']
        
        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            exc_name = exc_type.__name__ if exc_type else "Unknown"
            exc_msg = str(exc_value)[:100]
            return f"{timestamp} [{color}{level}{reset}] {exc_name}: {exc_msg}"
        
        return f"{timestamp} [{color}{level}{reset}] {message}"


class NetworkErrorFilter(logging.Filter):
    
    def __init__(self, max_same_errors=5):
        super().__init__()
        self.error_cache = {}
        self.max_same_errors = max_same_errors
    
    def filter(self, record):
        
        if record.levelno >= logging.ERROR:
            exc_info = record.exc_info
            if exc_info:
                exc_type = exc_info
                exc_name = exc_type.__name__ if exc_type else ""
                
                if exc_name in self.error_cache:
                    count, last_time = self.error_cache[exc_name]
                    now = datetime.now(timezone.utc).timestamp()
                     

                    if now - last_time > 60:  # Reset setelah 60 detik
                        self.error_cache[exc_name] = (1, now)
                    else:
                        self.error_cache[exc_name] = (count + 1, now)
                        if count >= self.max_same_errors:
                            return False
                else:
                    self.error_cache[exc_name] = (1, datetime.now(timezone.utc).timestamp())
        
        return True

# ============================================================
# === SETUP LOGGING (REPLACEMENT) ===
# ============================================================
_log_formatter = CompactFormatter()
_file_handler = RotatingFileHandler(
    "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_file_handler.setFormatter(_log_formatter)
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)

network_filter = NetworkErrorFilter(max_same_errors=3)
_stream_handler.addFilter(network_filter)
_file_handler.addFilter(network_filter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)