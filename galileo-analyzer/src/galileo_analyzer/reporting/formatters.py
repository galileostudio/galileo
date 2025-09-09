import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from ..core.models import JobAnalysisResult

class ReportGenerator:
    """Generate reports in different formats"""
    
    def __init__(self, output_dir: str = "reports"):
        self.reports_dir = Path(output_dir)
        self.reports_dir.mkdir(exist_ok=True)
    
    def generate_and_save_reports(self, results: List[JobAnalysisResult], provider: str, region: str):
        """Generate and save all report formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Generate summary
        summary = self._generate_summary(results, provider, region)
        
        # Create analysis data structure
        analysis_data = {
            'analysis_type': 'preliminary',
            'provider': provider,
            'region': region,
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'detailed_results': [self._result_to_dict(result) for result in results]
        }
        
        # Save different formats
        self._save_json_report(analysis_data, timestamp)
        self._save_csv_report(results, timestamp)
        self._save_candidates_list(summary.get('deep_analysis_candidates', []), timestamp)
        
        print(f"Reports saved to {self.reports_dir}/")
        print(f"  - JSON: preliminary_analysis_{timestamp}.json")
        print(f"  - CSV: jobs_inventory_{timestamp}.csv")
        print(f"  - Candidates: deep_analysis_candidates_{timestamp}.txt")
    
    def _generate_summary(self, results: List[JobAnalysisResult], provider: str, region: str) -> Dict[str, Any]:
        """Generate executive summary"""
        categories = {}
        total_cost = 0
        deep_analysis_candidates = []
        
        for result in results:
            # Count categories
            category = result.idle_analysis.category.value
            categories[category] = categories.get(category, 0) + 1
            
            # Sum costs
            cost = result.cost_estimate.estimated_monthly_brl
            total_cost += cost
            
            # Find candidates for deep analysis
            candidates = result.candidate_for_deep_analysis
            if any(candidates.values()):
                deep_analysis_candidates.append({
                    'job_name': result.job_name,
                    'reasons': [k for k, v in candidates.items() if v],
                    'cost': cost,
                    'category': category
                })
        
        # Calculate potential savings
        potential_savings = sum(
            r.cost_estimate.estimated_monthly_brl 
            for r in results 
            if r.idle_analysis.category.value in ['ABANDONED', 'NEVER_RUN']
        )
        
        return {
            'total_jobs': len(results),
            'successful_analyses': len([r for r in results if hasattr(r, 'job_name')]),
            'categories_distribution': categories,
            'cost_summary': {
                'total_monthly_brl': round(total_cost, 2),
                'potential_savings_brl': round(potential_savings, 2),
                'savings_percentage': round((potential_savings / total_cost * 100) if total_cost > 0 else 0, 1)
            },
            'deep_analysis_candidates': sorted(deep_analysis_candidates, 
                                             key=lambda x: x['cost'], reverse=True)[:20]
        }
    
    def _result_to_dict(self, result: JobAnalysisResult) -> Dict[str, Any]:
        """Convert JobAnalysisResult to dictionary for JSON serialization"""
        return {
            'job_name': result.job_name,
            'timestamp': result.timestamp.isoformat() if result.timestamp else None,
            'job_config': {
                'glue_version': result.job_config.glue_version,
                'worker_type': result.job_config.worker_type,
                'number_of_workers': result.job_config.number_of_workers,
                'timeout': result.job_config.timeout,
                'max_retries': result.job_config.max_retries
            },
            'idle_analysis': {
                'category': result.idle_analysis.category.value,
                'days_idle': result.idle_analysis.days_idle,
                'priority': result.idle_analysis.priority.value,
                'last_run_status': result.idle_analysis.last_run_status
            },
            'cost_estimate': {
                'hourly_cost_usd': result.cost_estimate.hourly_cost_usd,
                'estimated_monthly_usd': result.cost_estimate.estimated_monthly_usd,
                'estimated_monthly_brl': result.cost_estimate.estimated_monthly_brl
            },
            'tags_info': {
                'environment': result.tags_info.environment,
                'team': result.tags_info.team,
                'business_domain': result.tags_info.business_domain,
                'criticality': result.tags_info.criticality,
                'owner': result.tags_info.owner
            },
            'code_analysis': {
                'has_script': result.code_analysis.has_script,
                'script_location': result.code_analysis.script_location,
                'inferred_purpose': result.code_analysis.inferred_purpose,
                'naming_issues': result.code_analysis.naming_issues
            },
            'recent_runs_count': result.recent_runs_count,
            'candidate_for_deep_analysis': result.candidate_for_deep_analysis
        }
    
    def _save_json_report(self, analysis_data: Dict[str, Any], timestamp: str):
        """Save complete JSON report"""
        json_path = self.reports_dir / f"preliminary_analysis_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, indent=2, default=str, ensure_ascii=False)
    
    def _save_csv_report(self, results: List[JobAnalysisResult], timestamp: str):
        """Save CSV report for analysis"""
        csv_path = self.reports_dir / f"jobs_inventory_{timestamp}.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'job_name', 'category', 'days_idle', 'priority', 'monthly_cost_brl',
                'worker_type', 'environment', 'team', 'inferred_purpose',
                'deep_analysis_recommended', 'deep_analysis_reasons'
            ])
            
            for result in results:
                candidates = result.candidate_for_deep_analysis
                reasons = [k for k, v in candidates.items() if v]
                
                writer.writerow([
                    result.job_name,
                    result.idle_analysis.category.value,
                    result.idle_analysis.days_idle,
                    result.idle_analysis.priority.value,
                    result.cost_estimate.estimated_monthly_brl,
                    result.job_config.worker_type,
                    result.tags_info.environment,
                    result.tags_info.team,
                    result.code_analysis.inferred_purpose,
                    any(candidates.values()),
                    '; '.join(reasons)
                ])
    
    def _save_candidates_list(self, candidates: List[Dict], timestamp: str):
        """Save list of candidates for deep analysis"""
        candidates_path = self.reports_dir / f"deep_analysis_candidates_{timestamp}.txt"
        with open(candidates_path, 'w', encoding='utf-8') as f:
            f.write("# Deep Analysis Candidates\n")
            f.write(f"# Generated at: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n")
            
            for candidate in candidates:
                f.write(f"Job: {candidate['job_name']}\n")
                f.write(f"Category: {candidate['category']}\n")
                f.write(f"Cost: R$ {candidate['cost']:.2f}/month\n")
                f.write(f"Reasons: {', '.join(candidate['reasons'])}\n")
                f.write("-" * 50 + "\n\n")