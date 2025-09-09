import re
from typing import Dict, Any, List
from ...utils.validators import validate_job_name

class QuickCodeAnalyzer:
    """Quick code analysis without downloading scripts"""
    
    def quick_code_analysis(self, job_details: Dict[str, Any]) -> Dict[str, Any]:
        """Superficial analysis based on script location and job name only"""
        script_location = job_details.get('Command', {}).get('ScriptLocation', '')
        job_name = job_details.get('Name', '')
        
        analysis = {
            'has_script': bool(script_location),
            'script_location': script_location,
            'inferred_purpose': self._infer_purpose_from_name(job_name),
            'naming_issues': validate_job_name(job_name)
        }
        
        return analysis
    
    def _infer_purpose_from_name(self, job_name: str) -> str:
        """Infer job purpose from its name"""
        name_lower = job_name.lower()
        
        # ETL patterns
        if any(term in name_lower for term in ['etl', 'extract', 'transform', 'load', 'pipeline']):
            return 'ETL Pipeline'
        
        # Analytics patterns
        if any(term in name_lower for term in ['report', 'analytics', 'agg', 'dashboard', 'metric']):
            return 'Analytics/Reporting'
        
        # Data Quality patterns
        if any(term in name_lower for term in ['clean', 'quality', 'validate', 'check', 'audit']):
            return 'Data Quality'
        
        # Development/Testing patterns
        if any(term in name_lower for term in ['test', 'dev', 'temp', 'tmp', 'debug', 'sample']):
            return 'Development/Testing'
        
        # ML patterns
        if any(term in name_lower for term in ['model', 'train', 'predict', 'ml', 'ai', 'feature']):
            return 'Machine Learning'
        
        # Migration patterns
        if any(term in name_lower for term in ['migrate', 'migration', 'import', 'export', 'sync']):
            return 'Data Migration'
        
        return 'Unknown'