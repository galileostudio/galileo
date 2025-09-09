import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Dict, List, Any, Optional
from ..base import BaseCloudProvider
from ...core.config import AWSCredentials
from ...core.exceptions import AuthenticationError, ProviderError
from ...utils.logger import setup_logger


class GlueProvider(BaseCloudProvider):

    def __init__(self, credentials: AWSCredentials):
        super().__init__(credentials)
        self.region = credentials.region
        self.logger = setup_logger("galileo.providers.aws")
        self.authenticate()

    def authenticate(self) -> bool:
        """Authenticate with AWS and create clients"""
        try:
            creds = self.credentials

            if creds.profile:
                # Use AWS CLI profile
                self.logger.debug(f"Using AWS profile: {creds.profile}")
                session = boto3.Session(
                    profile_name=creds.profile, region_name=creds.region
                )
            else:
                # Use explicit credentials
                self.logger.debug("Using explicit AWS credentials")
                session = boto3.Session(
                    aws_access_key_id=creds.access_key_id,
                    aws_secret_access_key=creds.secret_access_key,
                    aws_session_token=creds.session_token,
                    region_name=creds.region,
                )

            # Create clients
            self.glue = session.client("glue")
            self.s3 = session.client("s3")
            self.cloudwatch = session.client("cloudwatch")

            # Test authentication
            self._test_authentication()
            self._client = self.glue

            self.logger.info(
                f"Successfully authenticated with AWS in region: {self.region}"
            )
            return True

        except (ClientError, NoCredentialsError) as e:
            self.logger.error(f"Failed to authenticate with AWS: {str(e)}")
            raise AuthenticationError(f"Failed to authenticate with AWS: {str(e)}")
        except Exception as e:
            self.logger.exception(f"Error initializing AWS provider: {str(e)}")
            raise ProviderError(f"Error initializing AWS provider: {str(e)}")

    def _test_authentication(self):
        """Test if credentials are valid by making a simple API call"""
        try:
            # FIXED: Use JobNames field
            response = self.glue.list_jobs(MaxResults=1)
            job_names = response.get("JobNames", [])
            self.logger.info(
                f"Authentication test successful - found {len(job_names)} jobs in first page"
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in [
                "InvalidUserID.NotFound",
                "AccessDenied",
                "UnauthorizedOperation",
            ]:
                self.logger.error(f"Invalid AWS credentials: {error_code}")
                raise AuthenticationError(f"Invalid AWS credentials: {error_code}")
            self.logger.error(f"AWS API error during authentication test: {error_code}")
            raise

    def get_all_jobs(self) -> List[str]:
        """List all Glue jobs with proper pagination"""
        if not self.is_authenticated():
            raise AuthenticationError("Provider not authenticated")

        all_job_names = []
        next_token = None
        page_count = 0

        try:
            self.logger.info("Starting to list all Glue jobs")

            while True:
                page_count += 1

                # Build request parameters
                params = {"MaxResults": 100}
                if next_token:
                    params["NextToken"] = next_token

                # Make API call
                response = self.glue.list_jobs(**params)

                # FIXED: Use JobNames instead of Jobs
                page_jobs = response.get("JobNames", [])
                all_job_names.extend(page_jobs)

                self.logger.debug(
                    f"Retrieved page {page_count}: {len(page_jobs)} jobs (total: {len(all_job_names)})"
                )

                # Check for next page
                next_token = response.get("NextToken")
                if not next_token:
                    break

                # Safety break
                if page_count > 50:
                    self.logger.warning(
                        f"Stopped after {page_count} pages (safety limit)"
                    )
                    break

            self.logger.info(
                f"Pagination complete: {len(all_job_names)} total jobs found"
            )
            return all_job_names

        except ClientError as e:
            self.logger.error(f"Failed to list jobs: {str(e)}")
            raise ProviderError(f"Failed to list jobs: {str(e)}")

    def get_job_details(self, job_name: str) -> Dict[str, Any]:
        """Get detailed job information"""
        if not self.is_authenticated():
            raise AuthenticationError("Provider not authenticated")

        try:
            self.logger.debug(f"Fetching details for job: {job_name}")
            response = self.glue.get_job(JobName=job_name)
            self.logger.debug(f"Successfully retrieved details for job: {job_name}")
            return response["Job"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                self.logger.warning(f"Job '{job_name}' not found")
                raise ProviderError(f"Job '{job_name}' not found")
            self.logger.error(f"Failed to get job details for {job_name}: {str(e)}")
            raise ProviderError(f"Failed to get job details: {str(e)}")

    def get_recent_runs(
        self, job_name: str, max_results: int = 5
    ) -> List[Dict[str, Any]]:
        """Get recent job executions"""
        if not self.is_authenticated():
            raise AuthenticationError("Provider not authenticated")

        try:
            self.logger.debug(f"Fetching recent runs for job: {job_name}")
            response = self.glue.get_job_runs(JobName=job_name, MaxResults=max_results)
            runs = response["JobRuns"]
            self.logger.debug(f"Found {len(runs)} recent runs for job: {job_name}")
            return runs
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                self.logger.debug(f"No runs found for job: {job_name} (job exists)")
                return []  # Job exists but no runs
            self.logger.error(f"Failed to get job runs for {job_name}: {str(e)}")
            raise ProviderError(f"Failed to get job runs: {str(e)}")

    def _extract_job_config(self, job_details: Dict[str, Any]):
        """Extract job configuration from job details"""
        return JobConfig(
            glue_version=job_details.get("GlueVersion"),
            worker_type=job_details.get("WorkerType"),
            number_of_workers=job_details.get("NumberOfWorkers"),
            timeout=job_details.get("Timeout"),
            max_retries=job_details.get("MaxRetries"),
        )
