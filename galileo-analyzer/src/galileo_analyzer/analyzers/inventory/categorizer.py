from datetime import datetime
from typing import Dict, List, Any
from ...core.models import IdleAnalysis, JobCategory, Priority

class JobCategorizer:
    
    @staticmethod
    def categorize_by_idle_time(job_details: Dict, job_runs: List[Dict]) -> IdleAnalysis:
        """Categorização básica por tempo de inatividade"""
        now = datetime.now().replace(tzinfo=None)
        
        if not job_runs:
            created_time = job_details.get('CreatedOn', now).replace(tzinfo=None)
            days_since_creation = (now - created_time).days
            return IdleAnalysis(
                category=JobCategory.NEVER_RUN,
                days_idle=days_since_creation,
                priority=Priority.CRITICAL,
                last_run_status=None
            )
        
        last_run = max(job_runs, key=lambda x: x['StartedOn'])
        last_run_time = last_run['StartedOn'].replace(tzinfo=None)
        days_idle = (now - last_run_time).days
        
        if days_idle <= 7:
            category, priority = JobCategory.ACTIVE, Priority.LOW
        elif days_idle <= 30:
            category, priority = JobCategory.RECENT, Priority.LOW
        elif days_idle <= 90:
            category, priority = JobCategory.INACTIVE, Priority.MEDIUM
        else:
            category, priority = JobCategory.ABANDONED, Priority.HIGH
        
        return IdleAnalysis(
            category=category,
            days_idle=days_idle,
            priority=priority,
            last_run_status=last_run['JobRunState']
        )

    @staticmethod
    def should_analyze_deeply(
        job_details: Dict, 
        job_runs: List[Dict],
        cost_estimate: 'CostEstimate',
        idle_analysis: IdleAnalysis,
        code_analysis: 'QuickCodeAnalysis',
        tags_info: 'TagsInfo'
    ) -> Dict[str, bool]:
        """Determina se job deve passar por análise profunda"""
        return {
            'high_cost': cost_estimate.estimated_monthly_brl > 500,
            'inactive_expensive': (
                idle_analysis.category in [JobCategory.ABANDONED, JobCategory.INACTIVE] and 
                cost_estimate.estimated_monthly_brl > 100
            ),
            'never_run': idle_analysis.category == JobCategory.NEVER_RUN,
            'naming_issues': len(code_analysis.naming_issues) > 0,
            'dev_in_prod': (
                tags_info.environment in ['dev', 'test'] or 
                'test' in job_details.get('Name', '').lower()
            )
        }