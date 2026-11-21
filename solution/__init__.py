"""受管工位连接事件模拟器。

命令：``python -m solution <ingest|report|state> CASE.json [--db DB.sqlite]``。
"""

from .engine import Ctx  # noqa: F401
from .model import Case, CaseError, normalize  # noqa: F401

__all__ = ["Case", "CaseError", "Ctx", "normalize"]
__version__ = "1.0.0"
