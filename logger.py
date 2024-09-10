import logging
import os
import sys

colors = {
    'black': "\x1b[30m",
    'red': "\x1b[31m",
    'green': "\x1b[32m",
    'yellow': "\x1b[33m",
    'blue': "\x1b[34m",
    'magenta': "\x1b[35m",
    'cyan': "\x1b[36m",
    'white': "\x1b[37m",
    'bright_black': "\x1b[90m",
    'bright_red': "\x1b[91m",
    'bright_green': "\x1b[92m",
    'bright_yellow': "\x1b[93m",
    'bright_blue': "\x1b[94m",
    'bright_magenta': "\x1b[95m",
    'bright_cyan': "\x1b[96m",
    'bright_white': "\x1b[97m",
    'reset': "\x1b[0m"
}

level_colors = {
    'DEBUG': colors['bright_blue'],
    'INFO': colors['bright_white'],
    'WARNING': colors['bright_yellow'],
    'ERROR': colors['bright_red'],
    'CRITICAL': colors['magenta']
}

message_colors = {
    'DEBUG': colors['bright_cyan'],
    'INFO': colors['green'],
    'WARNING': colors['yellow'],
    'ERROR': colors['red'],
    'CRITICAL': colors['bright_magenta']
}


class CustomFormatter(logging.Formatter):
    def format(self, record):
        level_color = level_colors.get(record.levelname, colors['white'])
        message_color = message_colors.get(record.levelname, colors['white'])
        reset_color = colors['reset']
        log_format = (
            f"{colors['blue']}%(pathname)s{reset_color}:%(lineno)d{reset_color}: "
            f"{colors['red']}%(asctime)s{reset_color} "
            f"{colors['cyan']}%(name)s{reset_color} "
            f"{level_color}[%(levelname)s]{reset_color} "
            f"{message_color}%(message)s{reset_color}"
        )
        formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setFormatter(CustomFormatter())
handlers = [stdout_handler]

log_filename = os.getenv("LOG_FILENAME", f"{__file__}.log")
try:
    file_handler = logging.FileHandler(filename=log_filename)
    file_handler.setFormatter(CustomFormatter())
    handlers.append(file_handler)
except (OSError, IOError) as e:
    print(f"Failed to create file handler: {e}", file=sys.stderr)

logging.basicConfig(
    level=os.getenv("CE_LOG_LEVEL", "INFO").upper(),
    handlers=handlers,
    force=True
)

logger = logging.getLogger(__name__)

# logger.debug("Debug message")
# logger.info("Info message")
# logger.warning("Warning message")
# logger.error("Error message")
# logger.critical("Critical message")
