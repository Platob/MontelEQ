from __future__ import annotations

from yggdrasil import VersionInfo

__all__ = [
    "__version_info__",
    "__version__"
]

__version__ = "0.1.30"
__version_info__ = VersionInfo.from_string(__version__)
