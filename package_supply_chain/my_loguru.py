from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("application.log", level="INFO")

