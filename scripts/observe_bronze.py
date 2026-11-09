"""Generate the Bronze business quality observability report."""

import json
from pathlib import Path

from commercelake.quality.bronze_observability import observe_bronze


def main():
    """Calculate and persist Bronze observability metrics."""
    result = observe_bronze()

    report_path = Path("reports/bronze_observability_report.json")

    report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path.write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
