from __future__ import annotations

from yggdrasil import VersionInfo

__all__ = [
    "__version_info__",
    "__version__"
]

__version__ = "0.2.0"
__version_info__ = VersionInfo.from_string(__version__)
