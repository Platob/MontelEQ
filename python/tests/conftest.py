import importlib
import importlib.util
import logging
import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _bootstrap_monteleq():
    """Bootstrap monteleq.model and monteleq.api.request for unit testing.

    In environments where the full yggdrasil native module is unavailable,
    importing through ``monteleq.__init__`` fails because it chains to
    ``APIClient`` which requires ``yggdrasil.io.http_.HTTPSession``.

    This bootstraps the package and subpackage modules directly, so that
    ``from monteleq.model import Curve`` works without triggering the
    APIClient import chain.
    """
    if "monteleq" in sys.modules:
        return

    try:
        import monteleq  # noqa: F401
        return
    except (ImportError, ModuleNotFoundError):
        pass

    pkg = types.ModuleType("monteleq")
    pkg.__path__ = [str(SRC_ROOT / "monteleq")]
    sys.modules["monteleq"] = pkg

    api_pkg = types.ModuleType("monteleq.api")
    api_pkg.__path__ = [str(SRC_ROOT / "monteleq" / "api")]
    sys.modules["monteleq.api"] = api_pkg

    model_path = SRC_ROOT / "monteleq" / "model.py"
    if model_path.exists():
        spec = importlib.util.spec_from_file_location("monteleq.model", str(model_path))
        mod = importlib.util.module_from_spec(spec)
        sys.modules["monteleq.model"] = mod
        spec.loader.exec_module(mod)

    request_path = SRC_ROOT / "monteleq" / "api" / "request.py"
    if request_path.exists():
        spec = importlib.util.spec_from_file_location("monteleq.api.request", str(request_path))
        mod = importlib.util.module_from_spec(spec)
        sys.modules["monteleq.api.request"] = mod
        spec.loader.exec_module(mod)


_bootstrap_monteleq()

logger = logging.getLogger("monteleq")


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
