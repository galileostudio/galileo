import argparse
import re
import sys
from pathlib import Path
from ..providers.factory import ProviderFactory
from ..core.config import AWSCredentials
from ..analyzers.inventory.scanner import InventoryScanner
from ..reporting.formatters import ReportGenerator
from ..core.exceptions import AuthenticationError, ConfigurationError
from ..utils.logger import setup_logger


def main():
    # Setup logger first
    logger = setup_logger("galileo.cli", "INFO")

    parser = argparse.ArgumentParser(
        description="Galileo - Cloud Data Jobs Inventory Analysis"
    )

    # Provider selection
    parser.add_argument(
        "--provider",
        required=True,
        choices=["aws", "databricks", "snowflake"],
        help="Cloud provider to analyze",
    )

    # AWS-specific arguments
    parser.add_argument("--region", help="AWS region (default: us-east-1)")
    parser.add_argument("--profile", help="AWS CLI profile name")
    parser.add_argument("--access-key-id", help="AWS Access Key ID")
    parser.add_argument("--secret-access-key", help="AWS Secret Access Key")
    parser.add_argument(
        "--session-token", help="AWS Session Token (for temporary credentials)"
    )

    # Logging arguments
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument("--log-file", help="Path to log file")

    # General arguments
    parser.add_argument("--job-filter", help="Regex to filter jobs")
    parser.add_argument(
        "--output-dir", default="reports", help="Output directory for reports"
    )

    args = parser.parse_args()

    # Reconfigure logger with user-provided settings
    logger = setup_logger(
        "galileo.cli", args.log_level, Path(args.log_file) if args.log_file else None
    )

    try:
        logger.info(f"Starting Galileo analysis for provider: {args.provider}")

        # Create credentials based on provider
        if args.provider == "aws":
            credentials = create_aws_credentials(args)
            provider = ProviderFactory.create_aws_provider(credentials)
            logger.info(f"AWS provider initialized for region: {provider.region}")
        else:
            raise ConfigurationError(f"Provider {args.provider} not yet implemented")

        # Initialize scanner
        scanner = InventoryScanner(provider)
        report_generator = ReportGenerator(args.output_dir)

        # Filter jobs if specified
        all_jobs = provider.get_all_jobs()
        logger.info(f"Found {len(all_jobs)} jobs total")

        if args.job_filter:
            filter_regex = re.compile(args.job_filter, re.I)
            all_jobs = [job for job in all_jobs if filter_regex.search(job)]
            logger.info(f"Filter applied: {len(all_jobs)} jobs selected")

        # Execute analysis
        logger.info("Starting job analysis...")
        results = scanner.scan_jobs(all_jobs)

        # Generate and save reports
        logger.info("Generating reports...")
        report_generator.generate_and_save_reports(
            results, args.provider, provider.region
        )

        logger.info("Analysis completed successfully!")

    except (AuthenticationError, ConfigurationError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error during analysis: {e}")
        sys.exit(1)


def create_aws_credentials(args) -> AWSCredentials:
    """Create AWS credentials from command line arguments"""

    # Priority 1: Explicit credentials via command line
    if args.access_key_id and args.secret_access_key:
        return AWSCredentials.from_params(
            access_key_id=args.access_key_id,
            secret_access_key=args.secret_access_key,
            session_token=args.session_token,
            region=args.region or "us-east-1",
        )

    # Priority 2: Profile from command line
    if args.profile:
        return AWSCredentials.from_profile(
            profile=args.profile, region=args.region or "us-east-1"
        )

    # Priority 3: Environment variables
    env_creds = AWSCredentials.from_env()
    if env_creds.access_key_id and env_creds.secret_access_key:
        # Override region if provided
        if args.region:
            env_creds.region = args.region
        return env_creds

    # Priority 4: Default profile
    return AWSCredentials.from_profile(
        profile="default", region=args.region or "us-east-1"
    )


if __name__ == "__main__":
    main()
