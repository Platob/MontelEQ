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

_BOOTSTRAP_MODULES = [
    "monteleq.model",
    "monteleq.api.request",
    "monteleq.api.schemas",
    "monteleq.api.curation_helpers",
    "monteleq.api.curation_client",
]


def _bootstrap_monteleq():
    """Bootstrap monteleq submodules for unit testing.

    When the full yggdrasil native module is unavailable, importing through
    ``monteleq.__init__`` fails because it chains to ``APIClient`` which
    requires ``yggdrasil.io.http_.HTTPSession``.  This bootstraps individual
    modules directly so tests can import them without the full chain.
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

    for mod_name in _BOOTSTRAP_MODULES:
        parts = mod_name.split(".")
        file_path = SRC_ROOT / ("/".join(parts) + ".py")
        if not file_path.exists():
            continue
        spec = importlib.util.spec_from_file_location(mod_name, str(file_path))
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception:
            del sys.modules[mod_name]


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
