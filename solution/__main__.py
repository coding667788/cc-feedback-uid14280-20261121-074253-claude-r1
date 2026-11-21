"""受管工位连接事件模拟器。

包入口：python -m solution <命令> CASE.json [--db DB.sqlite] [--out FILE.json]
"""

import sys

from .cli import main

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
