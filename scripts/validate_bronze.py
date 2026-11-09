"""Execute Bronze quality validation and save its report."""

import json
import sys
from pathlib import Path

from commercelake.quality.bronze_validator import validate_bronze


def main():
    """Validate the active Bronze dataset."""
    result = validate_bronze()

    report_path = Path("reports/bronze_quality_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_path.write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(result, indent=2))

    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
