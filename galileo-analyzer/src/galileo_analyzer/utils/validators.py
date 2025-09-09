import re
from typing import List


def validate_job_name(job_name: str) -> List[str]:
    """Validate job naming conventions and return issues"""
    issues = []

    if len(job_name) < 3:
        issues.append("Job name too short (less than 3 characters)")

    if len(job_name) > 255:
        issues.append("Job name too long (more than 255 characters)")

    # Check for development/test patterns in production
    dev_patterns = ["test", "tmp", "temp", "dev", "debug", "sample"]
    name_lower = job_name.lower()
    for pattern in dev_patterns:
        if pattern in name_lower:
            issues.append(f"Development/test pattern '{pattern}' found in job name")

    # Check naming convention
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_-]*$", job_name):
        issues.append(
            "Job name doesn't follow standard naming convention (should start with letter, contain only letters, numbers, underscores, hyphens)"
        )

    # Check for spaces
    if " " in job_name:
        issues.append("Job name contains spaces")

    return issues


def validate_aws_region(region: str) -> bool:
    """Validate AWS region format"""
    # Basic AWS region pattern: us-east-1, eu-west-1, etc.
    pattern = r"^[a-z]{2}-[a-z]+-\d+$"
    return bool(re.match(pattern, region))


def validate_s3_path(s3_path: str) -> bool:
    """Validate S3 path format"""
    # Basic S3 path pattern: s3://bucket-name/path/to/file
    pattern = r"^s3://[a-z0-9.-]+/.+$"
    return bool(re.match(pattern, s3_path))
