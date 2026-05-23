"""CLI entry point for monteleq pipeline operations."""
import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="monteleq",
        description="MontelEQ ingestion pipeline CLI",
    )
    sub = parser.add_subparsers(dest="command")

    ingest = sub.add_parser("ingest", help="Run ingestion for one or more data types")
    ingest.add_argument("data_types", nargs="*", help="Data types to ingest (default: all)")
    ingest.add_argument("--catalog", default="trading_tgp_prd")
    ingest.add_argument("--schema", default="src_monteleq")
    ingest.add_argument("--period-days", type=int, default=60)
    ingest.add_argument("--issued-lookback-days", type=int, default=None)

    deploy = sub.add_parser("deploy", help="Deploy Databricks job for all data types")
    deploy.add_argument("--job-name", default="monteleq-ingestion")
    deploy.add_argument("--cluster-id", default="0522-063219-lfvirtho")
    deploy.add_argument("--catalog", default="trading_tgp_prd")
    deploy.add_argument("--schema", default="src_monteleq")
    deploy.add_argument("--period-days", type=int, default=60)
    deploy.add_argument("--schedule", default="0 0 5 * * ?")
    deploy.add_argument("data_types", nargs="*", help="Data types (default: all)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    if args.command == "ingest":
        from monteleq.pipeline import run_pipeline
        results = run_pipeline(
            data_types=args.data_types or None,
            catalog_name=args.catalog,
            schema_name=args.schema,
            period_days=args.period_days,
            issued_at_lookback_days=args.issued_lookback_days,
        )
        for r in results:
            print(r)

    elif args.command == "deploy":
        from monteleq.pipeline import deploy_pipeline
        job = deploy_pipeline(
            job_name=args.job_name,
            cluster_id=args.cluster_id,
            catalog_name=args.catalog,
            schema_name=args.schema,
            period_days=args.period_days,
            schedule_cron=args.schedule,
            data_types=args.data_types or None,
        )
        print(f"Job deployed: {job}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
