"""Command-line entry point for retail ingestion."""

import argparse
import json

from commercelake.ingestion.pipeline import ingest


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Online Retail II into Parquet."
    )
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()

    manifest = ingest(
        batch_size=args.batch_size,
        max_rows=args.max_rows,
    )

    print("\n=== INGESTION COMPLETE ===")
    print(
        json.dumps(
            {
                "run_id": manifest["run_id"],
                "total_rows": manifest["total_rows"],
                "parquet_files": len(manifest["files"]),
                "partitions": len(manifest["partitions"]),
                "complete_source": manifest["complete_source"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
