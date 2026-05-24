import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "python" / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

logger = logging.getLogger("yggdrasil")


def setup_logging(level: int = logging.INFO) -> None:
    if logger.handlers:
        return

    logger.setLevel(level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    fmt = (
        "%(asctime)s | %(levelname)s | %(name)s | "
        "%(filename)s:%(lineno)d | %(funcName)s | %(message)s"
    )
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)


setup_logging(logging.DEBUG)
