"""
    Initializes a logger with the given file path and returns the logger instance.
    Author : haja
"""
import logging
import os


# Define a custom formatter that uses ANSI escape codes to color the output
class ColorFormatter(logging.Formatter):
    red = '\033[31m'
    green = '\033[32m'
    yellow = '\033[33m'
    blue = '\033[34m'
    magenta = '\033[35m'
    cyan = '\033[36m'
    reset = '\033[0m'

    def format(self, record):
        if record.levelno == logging.ERROR:
            record.msg = f"{self.red}{record.msg}{self.reset}"
        elif record.levelno == logging.WARNING:
            record.msg = f"{self.yellow}{record.msg}{self.reset}"
        elif record.levelno == logging.INFO:
            record.msg = f"{self.green}{record.msg}{self.reset}"
        elif record.levelno == logging.DEBUG:
            record.msg = f"{self.blue}{record.msg}{self.reset}"
        return super().format(record)


logger = logging.getLogger("sanity_check_dashboard_logger")

log_file_path = os.path.normpath(os.path.join("sanity_check_dashboard.log"))
# Create a logger instance with the root logger's configuration
general_logger = logging.getLogger()

# Set the logging level to DEBUG
general_logger.setLevel(logging.DEBUG)

# Create a stream handler for logging to the console
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler("sanity_check_dashboard.log")

# Set the logging level for the stream handler to DEBUG
stream_handler.setLevel(logging.DEBUG)
file_handler.setLevel(logging.DEBUG)

# Create a formatter that includes the timestamp, logging level, and log message
# formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')

# Set the formatter for the stream handler

stream_handler.setFormatter(ColorFormatter("%(asctime)s - [%(levelname)s] : %(message)s"))
file_handler.setFormatter(ColorFormatter("%(asctime)s - [%(levelname)s] : %(message)s"))
# Add the stream handler to the logger instance
general_logger.addHandler(stream_handler)
general_logger.addHandler(file_handler)
