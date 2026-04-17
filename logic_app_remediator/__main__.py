"""python -m logic_app_remediator"""

import sys

from logic_app_remediator.cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
