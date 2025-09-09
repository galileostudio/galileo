from typing import Dict, Any
from ..core.models import TagsInfo  # ADD THIS IMPORT

def extract_tags_info(job_details: Dict[str, Any]) -> TagsInfo:
    """Extract information from tags for categorization"""
    tags = job_details.get('Tags', {})
    default_args = job_details.get('DefaultArguments', {})
    
    # Tags can be in DefaultArguments with prefix --
    for key, value in default_args.items():
        if key.startswith('--tag-'):
            tag_name = key[6:]  # Remove '--tag-'
            tags[tag_name] = value
    
    # CHANGE THIS: Return dataclass instead of dict
    return TagsInfo(
        environment=tags.get('Environment', tags.get('env', 'unknown')).lower(),
        team=tags.get('Team', tags.get('team', 'unknown')).lower(),
        business_domain=tags.get('BusinessDomain', tags.get('domain', 'unknown')).lower(),
        criticality=tags.get('Criticality', tags.get('criticality', 'unknown')).lower(),
        owner=tags.get('Owner', tags.get('owner', 'unknown')).lower()
    )