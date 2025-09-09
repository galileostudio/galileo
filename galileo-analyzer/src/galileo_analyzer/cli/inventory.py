import argparse
import re
import os
from ..providers.factory import ProviderFactory
from ..core.config import AWSCredentials
from ..analyzers.inventory.scanner import InventoryScanner
from ..reporting.formatters import ReportGenerator
from ..core.exceptions import AuthenticationError, ConfigurationError

def main():
    parser = argparse.ArgumentParser(description='Galileo - Cloud Data Jobs Inventory Analysis')
    
    # Provider selection
    parser.add_argument('--provider', required=True, choices=['aws', 'databricks', 'snowflake'], 
                       help='Cloud provider to analyze')
    
    # AWS-specific arguments
    parser.add_argument('--region', help='AWS region (default: us-east-1)')
    parser.add_argument('--profile', help='AWS CLI profile name')
    parser.add_argument('--access-key-id', help='AWS Access Key ID')
    parser.add_argument('--secret-access-key', help='AWS Secret Access Key')
    parser.add_argument('--session-token', help='AWS Session Token (for temporary credentials)')
    
    # General arguments
    parser.add_argument('--job-filter', help='Regex to filter jobs')
    parser.add_argument('--output-dir', default='reports', help='Output directory for reports')
    
    args = parser.parse_args()
    
    try:
        # Create credentials based on provider
        if args.provider == 'aws':
            credentials = create_aws_credentials(args)
            provider = ProviderFactory.create_aws_provider(credentials)
        else:
            raise ConfigurationError(f"Provider {args.provider} not yet implemented")
        
        # Initialize scanner
        scanner = InventoryScanner(provider)
        report_generator = ReportGenerator(args.output_dir)
        
        # Filter jobs if specified
        all_jobs = provider.get_all_jobs()
        if args.job_filter:
            filter_regex = re.compile(args.job_filter, re.I)
            all_jobs = [job for job in all_jobs if filter_regex.search(job)]
            print(f"Filter applied: {len(all_jobs)} jobs selected")
        
        # Execute analysis
        results = scanner.scan_jobs(all_jobs)
        
        # Generate and save reports
        report_generator.generate_and_save_reports(results, args.provider, provider.region)
        
    except (AuthenticationError, ConfigurationError) as e:
        print(f"Error: {e}")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)

def create_aws_credentials(args) -> AWSCredentials:
    """Create AWS credentials from command line arguments"""
    
    # Priority 1: Explicit credentials via command line
    if args.access_key_id and args.secret_access_key:
        return AWSCredentials.from_params(
            access_key_id=args.access_key_id,
            secret_access_key=args.secret_access_key,
            session_token=args.session_token,
            region=args.region or 'us-east-1'
        )
    
    # Priority 2: Profile from command line
    if args.profile:
        return AWSCredentials.from_profile(
            profile=args.profile,
            region=args.region or 'us-east-1'
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
        profile='default',
        region=args.region or 'us-east-1'
    )

if __name__ == '__main__':
    main()