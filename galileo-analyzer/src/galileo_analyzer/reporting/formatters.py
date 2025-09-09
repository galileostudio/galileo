import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from ..core.models import JobAnalysisResult
from ..utils.logger import setup_logger
from ..core.models import DeepAnalysisResult


class ReportGenerator:
    """Generate reports in different formats"""

    def __init__(self, output_dir: str = "reports"):
        self.reports_dir = Path(output_dir)
        self.reports_dir.mkdir(exist_ok=True)
        self.logger = setup_logger("galileo.reporting")

    # Modificar este método para incluir o save dos candidatos JSON:

    def generate_and_save_reports(
        self, results: List[JobAnalysisResult], provider: str, region: str
    ):
        """Generate and save all report formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate summary
        summary = self._generate_summary(results, provider, region)

        # Create analysis data structure
        analysis_data = {
            "analysis_type": "preliminary",
            "provider": provider,
            "region": region,
            "timestamp": datetime.now().isoformat(),
            "summary": summary,
            "detailed_results": [self._result_to_dict(result) for result in results],
        }

        # Save different formats
        self._save_json_report(analysis_data, timestamp)
        self._save_csv_report(results, timestamp)
        self._save_candidates_list(
            summary.get("deep_analysis_candidates", []), timestamp
        )

        # ADICIONAR ESTA LINHA - salvar candidatos em JSON para deep analysis
        self._save_candidates_json(
            summary.get("deep_analysis_candidates", []), timestamp
        )

        self.logger.info(f"Reports saved to {self.reports_dir}/")
        self.logger.info(f"  - JSON: preliminary_analysis_{timestamp}.json")
        self.logger.info(f"  - CSV: jobs_inventory_{timestamp}.csv")
        self.logger.info(f"  - Candidates: deep_analysis_candidates_{timestamp}.txt")
        self.logger.info(
            f"  - Candidates JSON: deep_analysis_candidates_{timestamp}.json"
        )

    def _generate_summary(
        self, results: List[JobAnalysisResult], provider: str, region: str
    ) -> Dict[str, Any]:
        """Generate executive summary"""
        self.logger.debug("Generating executive summary")
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
                deep_analysis_candidates.append(
                    {
                        "job_name": result.job_name,
                        "reasons": [k for k, v in candidates.items() if v],
                        "cost": cost,
                        "category": category,
                    }
                )

        # Calculate potential savings
        potential_savings = sum(
            r.cost_estimate.estimated_monthly_brl
            for r in results
            if r.idle_analysis.category.value in ["ABANDONED", "NEVER_RUN"]
        )

        summary_data = {
            "total_jobs": len(results),
            "successful_analyses": len([r for r in results if hasattr(r, "job_name")]),
            "categories_distribution": categories,
            "cost_summary": {
                "total_monthly_brl": round(total_cost, 2),
                "potential_savings_brl": round(potential_savings, 2),
                "savings_percentage": round(
                    (potential_savings / total_cost * 100) if total_cost > 0 else 0, 1
                ),
            },
            "deep_analysis_candidates": sorted(
                deep_analysis_candidates, key=lambda x: x["cost"], reverse=True
            )[:20],
        }

        self.logger.info(
            f"Summary generated: {len(results)} jobs, "
            f"R$ {total_cost:.2f}/month total, "
            f"R$ {potential_savings:.2f}/month potential savings"
        )
        return summary_data

    def _result_to_dict(self, result: JobAnalysisResult) -> Dict[str, Any]:
        """Convert JobAnalysisResult to dictionary for JSON serialization"""
        return {
            "job_name": result.job_name,
            "timestamp": result.timestamp.isoformat() if result.timestamp else None,
            "job_config": {
                "glue_version": result.job_config.glue_version,
                "worker_type": result.job_config.worker_type,
                "number_of_workers": result.job_config.number_of_workers,
                "timeout": result.job_config.timeout,
                "max_retries": result.job_config.max_retries,
                "max_capacity": result.job_config.max_capacity,
                "allocated_capacity": result.job_config.allocated_capacity,
                "description": result.job_config.description,
                "job_mode": result.job_config.job_mode,
                "job_type": result.job_config.job_type,
                "execution_class": result.job_config.execution_class,
                "script_type": result.job_config.script_type,
                "script_location": result.job_config.script_location,
                "avg_execution_time_minutes": result.job_config.avg_execution_time_minutes,
            },
            "idle_analysis": {
                "category": result.idle_analysis.category.value,
                "days_idle": result.idle_analysis.days_idle,
                "priority": result.idle_analysis.priority.value,
                "last_run_status": result.idle_analysis.last_run_status,
            },
            "cost_estimate": {
                "hourly_cost_usd": result.cost_estimate.hourly_cost_usd,
                "estimated_monthly_usd": result.cost_estimate.estimated_monthly_usd,
                "estimated_monthly_brl": result.cost_estimate.estimated_monthly_brl,
            },
            "tags_info": {
                "environment": result.tags_info.environment,
                "team": result.tags_info.team,
                "business_domain": result.tags_info.business_domain,
                "criticality": result.tags_info.criticality,
                "owner": result.tags_info.owner,
            },
            "code_analysis": {
                "has_script": result.code_analysis.has_script,
                "script_location": result.code_analysis.script_location,
                "inferred_purpose": result.code_analysis.inferred_purpose,
                "naming_issues": result.code_analysis.naming_issues,
            },
            "recent_runs_count": result.recent_runs_count,
            "candidate_for_deep_analysis": result.candidate_for_deep_analysis,
        }

    def _save_json_report(self, analysis_data: Dict[str, Any], timestamp: str):
        """Save complete JSON report"""
        json_path = self.reports_dir / f"preliminary_analysis_{timestamp}.json"
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(analysis_data, f, indent=2, default=str, ensure_ascii=False)
            self.logger.debug(f"JSON report saved: {json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save JSON report: {e}")
            raise

    def _save_csv_report(self, results: List[JobAnalysisResult], timestamp: str):
        """Save CSV report for analysis with enhanced fields"""
        csv_path = self.reports_dir / f"jobs_inventory_{timestamp}.csv"
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "job_name",
                        "category",
                        "days_idle",
                        "priority",
                        "monthly_cost_brl",
                        "worker_type",
                        "number_of_workers",
                        "max_capacity",
                        "allocated_capacity",
                        "job_type",
                        "execution_class",
                        "script_type",
                        "script_location",
                        "avg_execution_minutes",
                        "glue_version",
                        "description",
                        "environment",
                        "team",
                        "inferred_purpose",
                        "deep_analysis_recommended",
                        "deep_analysis_reasons",
                    ]
                )

                for result in results:
                    candidates = result.candidate_for_deep_analysis
                    reasons = [k for k, v in candidates.items() if v]

                    writer.writerow(
                        [
                            result.job_name,
                            result.idle_analysis.category.value,
                            result.idle_analysis.days_idle,
                            result.idle_analysis.priority.value,
                            result.cost_estimate.estimated_monthly_brl,
                            result.job_config.worker_type,
                            result.job_config.number_of_workers,
                            result.job_config.max_capacity,
                            result.job_config.allocated_capacity,
                            result.job_config.job_type,
                            result.job_config.execution_class,
                            result.job_config.script_type,
                            result.job_config.script_location,
                            result.job_config.avg_execution_time_minutes,
                            result.job_config.glue_version,
                            result.job_config.description or "",
                            result.tags_info.environment,
                            result.tags_info.team,
                            result.code_analysis.inferred_purpose,
                            any(candidates.values()),
                            "; ".join(reasons),
                        ]
                    )
            self.logger.debug(f"CSV report saved: {csv_path}")
        except Exception as e:
            self.logger.error(f"Failed to save CSV report: {e}")
            raise

    def _save_candidates_list(self, candidates: List[Dict], timestamp: str):
        """Save list of candidates for deep analysis"""
        candidates_path = self.reports_dir / f"deep_analysis_candidates_{timestamp}.txt"
        try:
            with open(candidates_path, "w", encoding="utf-8") as f:
                f.write("# Deep Analysis Candidates\n")
                f.write(
                    f"# Generated at: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                )

                for candidate in candidates:
                    f.write(f"Job: {candidate['job_name']}\n")
                    f.write(f"Category: {candidate['category']}\n")
                    f.write(f"Cost: R$ {candidate['cost']:.2f}/month\n")
                    f.write(f"Reasons: {', '.join(candidate['reasons'])}\n")
                    f.write("-" * 50 + "\n\n")
            self.logger.debug(f"Candidates list saved: {candidates_path}")
            self.logger.info(f"Found {len(candidates)} candidates for deep analysis")
        except Exception as e:
            self.logger.error(f"Failed to save candidates list: {e}")
            raise

    def generate_deep_analysis_reports(
        self, results: List[DeepAnalysisResult], provider: str, region: str
    ):
        """Generate and save deep analysis reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate deep analysis summary
        summary = self._generate_deep_summary(results, provider, region)

        # Create deep analysis data structure
        analysis_data = {
            "analysis_type": "deep_analysis",
            "provider": provider,
            "region": region,
            "timestamp": datetime.now().isoformat(),
            "summary": summary,
            "detailed_results": [
                self._deep_result_to_dict(result) for result in results
            ],
        }

        # Save different formats
        self._save_deep_json_report(analysis_data, timestamp)
        self._save_deep_csv_report(results, timestamp)
        self._save_recommendations_report(results, timestamp)
        self._save_executive_summary(summary, timestamp)

        self.logger.info(f"Deep analysis reports saved to {self.reports_dir}/")
        self.logger.info(f"  - JSON: deep_analysis_{timestamp}.json")
        self.logger.info(f"  - CSV: deep_analysis_detailed_{timestamp}.csv")
        self.logger.info(f"  - Recommendations: recommendations_{timestamp}.md")
        self.logger.info(f"  - Executive Summary: executive_summary_{timestamp}.md")

    def _generate_deep_summary(
        self, results: List[DeepAnalysisResult], provider: str, region: str
    ) -> Dict[str, Any]:
        """Generate executive summary for deep analysis"""
        total_savings = 0
        high_priority_jobs = 0
        critical_issues = []
        top_recommendations = []

        for result in results:
            ai_recs = result.ai_recommendations

            # Sum potential savings
            if ai_recs.estimated_savings_brl:
                total_savings += ai_recs.estimated_savings_brl

            # Count high priority jobs
            if ai_recs.priority_score and ai_recs.priority_score > 70:
                high_priority_jobs += 1

            # Collect critical issues
            if ai_recs.risk_assessment and "HIGH" in ai_recs.risk_assessment:
                critical_issues.append(
                    {"job_name": result.job_name, "risk": ai_recs.risk_assessment}
                )

            # Collect top recommendations
            if ai_recs.cost_reduction_opportunities:
                for opportunity in ai_recs.cost_reduction_opportunities[
                    :2
                ]:  # Top 2 per job
                    top_recommendations.append(
                        {"job_name": result.job_name, "opportunity": opportunity}
                    )

        # Sort recommendations by potential impact
        top_recommendations = sorted(
            top_recommendations,
            key=lambda x: self._extract_savings_from_text(x["opportunity"]),
            reverse=True,
        )[:10]

        return {
            "total_jobs_analyzed": len(results),
            "total_estimated_savings_brl": round(total_savings, 2),
            "high_priority_jobs": high_priority_jobs,
            "critical_issues_count": len(critical_issues),
            "critical_issues": critical_issues[:5],  # Top 5
            "top_recommendations": top_recommendations,
            "analysis_summary": {
                "avg_analysis_duration": (
                    round(
                        sum(r.analysis_duration_seconds or 0 for r in results)
                        / len(results),
                        2,
                    )
                    if results
                    else 0
                ),
                "jobs_with_code_issues": len(
                    [r for r in results if r.code_analysis.performance_issues]
                ),
                "jobs_with_security_issues": len(
                    [r for r in results if r.code_analysis.security_issues]
                ),
            },
        }

    def _deep_result_to_dict(self, result: DeepAnalysisResult) -> Dict[str, Any]:
        """Convert DeepAnalysisResult to dictionary for JSON serialization"""
        return {
            "job_name": result.job_name,
            "timestamp": result.timestamp.isoformat() if result.timestamp else None,
            "analysis_duration_seconds": result.analysis_duration_seconds,
            "preliminary_analysis": self._result_to_dict(result.preliminary_result),
            "code_analysis": {
                "script_size_kb": result.code_analysis.script_size_kb,
                "complexity_score": result.code_analysis.complexity_score,
                "dependencies": result.code_analysis.dependencies,
                "sql_queries_count": result.code_analysis.sql_queries_count,
                "spark_operations": result.code_analysis.spark_operations,
                "performance_issues": result.code_analysis.performance_issues,
                "security_issues": result.code_analysis.security_issues,
                "best_practices_violations": result.code_analysis.best_practices_violations,
            },
            "performance_analysis": {
                "avg_cpu_utilization": result.performance_analysis.avg_cpu_utilization,
                "avg_memory_utilization": result.performance_analysis.avg_memory_utilization,
                "data_processed_gb": result.performance_analysis.data_processed_gb,
                "cost_per_gb": result.performance_analysis.cost_per_gb,
                "efficiency_score": result.performance_analysis.efficiency_score,
                "bottlenecks": result.performance_analysis.bottlenecks,
                "optimization_opportunities": result.performance_analysis.optimization_opportunities,
            },
            "dependency_analysis": {
                "input_sources": result.dependency_analysis.input_sources,
                "output_destinations": result.dependency_analysis.output_destinations,
                "upstream_jobs": result.dependency_analysis.upstream_jobs,
                "downstream_jobs": result.dependency_analysis.downstream_jobs,
                "schedule_conflicts": result.dependency_analysis.schedule_conflicts,
                "data_lineage": result.dependency_analysis.data_lineage,
            },
            "ai_recommendations": {
                "optimization_suggestions": result.ai_recommendations.optimization_suggestions,
                "cost_reduction_opportunities": result.ai_recommendations.cost_reduction_opportunities,
                "modernization_recommendations": result.ai_recommendations.modernization_recommendations,
                "risk_assessment": result.ai_recommendations.risk_assessment,
                "priority_score": result.ai_recommendations.priority_score,
                "estimated_savings_brl": result.ai_recommendations.estimated_savings_brl,
            },
        }

    def _save_deep_json_report(self, analysis_data: Dict[str, Any], timestamp: str):
        """Save complete deep analysis JSON report"""
        json_path = self.reports_dir / f"deep_analysis_{timestamp}.json"
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(analysis_data, f, indent=2, default=str, ensure_ascii=False)
            self.logger.debug(f"Deep analysis JSON report saved: {json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save deep analysis JSON report: {e}")
            raise

    def _save_deep_csv_report(self, results: List[DeepAnalysisResult], timestamp: str):
        """Save deep analysis CSV report"""
        csv_path = self.reports_dir / f"deep_analysis_detailed_{timestamp}.csv"
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "job_name",
                        "priority_score",
                        "estimated_savings_brl",
                        "risk_assessment",
                        "cpu_utilization",
                        "memory_utilization",
                        "efficiency_score",
                        "script_size_kb",
                        "complexity_score",
                        "performance_issues_count",
                        "security_issues_count",
                        "dependencies_count",
                        "bottlenecks_count",
                        "cost_reduction_opportunities",
                        "analysis_duration_seconds",
                    ]
                )

                for result in results:
                    writer.writerow(
                        [
                            result.job_name,
                            result.ai_recommendations.priority_score or 0,
                            result.ai_recommendations.estimated_savings_brl or 0,
                            result.ai_recommendations.risk_assessment or "Unknown",
                            result.performance_analysis.avg_cpu_utilization or 0,
                            result.performance_analysis.avg_memory_utilization or 0,
                            result.performance_analysis.efficiency_score or 0,
                            result.code_analysis.script_size_kb or 0,
                            result.code_analysis.complexity_score or 0,
                            len(result.code_analysis.performance_issues or []),
                            len(result.code_analysis.security_issues or []),
                            len(result.code_analysis.dependencies or []),
                            len(result.performance_analysis.bottlenecks or []),
                            "; ".join(
                                result.ai_recommendations.cost_reduction_opportunities
                                or []
                            ),
                            result.analysis_duration_seconds or 0,
                        ]
                    )
            self.logger.debug(f"Deep analysis CSV report saved: {csv_path}")
        except Exception as e:
            self.logger.error(f"Failed to save deep analysis CSV report: {e}")
            raise

    def _save_recommendations_report(
        self, results: List[DeepAnalysisResult], timestamp: str
    ):
        """Save actionable recommendations in Markdown format"""
        md_path = self.reports_dir / f"recommendations_{timestamp}.md"
        try:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write("# Deep Analysis Recommendations Report\n\n")
                f.write(
                    f"**Generated:** {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                )

                # Executive Summary
                total_savings = sum(
                    r.ai_recommendations.estimated_savings_brl or 0 for r in results
                )
                f.write(f"## Executive Summary\n\n")
                f.write(
                    f"- **Total Potential Savings:** R$ {total_savings:,.2f}/month\n"
                )
                f.write(f"- **Jobs Analyzed:** {len(results)}\n")
                f.write(
                    f"- **High Priority Jobs:** {len([r for r in results if (r.ai_recommendations.priority_score or 0) > 70])}\n\n"
                )

                # Sort by priority and savings
                sorted_results = sorted(
                    results,
                    key=lambda x: (
                        x.ai_recommendations.priority_score or 0,
                        x.ai_recommendations.estimated_savings_brl or 0,
                    ),
                    reverse=True,
                )

                # Top recommendations
                f.write("## Top Recommendations\n\n")
                for i, result in enumerate(sorted_results[:5], 1):
                    f.write(f"### {i}. {result.job_name}\n")
                    f.write(
                        f"**Priority Score:** {result.ai_recommendations.priority_score or 0}/100\n"
                    )
                    f.write(
                        f"**Potential Savings:** R$ {result.ai_recommendations.estimated_savings_brl or 0:,.2f}/month\n"
                    )
                    f.write(
                        f"**Risk Level:** {result.ai_recommendations.risk_assessment or 'Unknown'}\n\n"
                    )

                    if result.ai_recommendations.cost_reduction_opportunities:
                        f.write("**Cost Reduction Opportunities:**\n")
                        for (
                            opp
                        ) in result.ai_recommendations.cost_reduction_opportunities:
                            f.write(f"- {opp}\n")
                        f.write("\n")

                    if result.ai_recommendations.optimization_suggestions:
                        f.write("**Optimization Suggestions:**\n")
                        for (
                            suggestion
                        ) in result.ai_recommendations.optimization_suggestions[:3]:
                            f.write(f"- {suggestion}\n")
                        f.write("\n")

                    f.write("---\n\n")

                # Detailed Analysis
                f.write("## Detailed Analysis by Job\n\n")
                for result in sorted_results:
                    f.write(f"### {result.job_name}\n\n")

                    # Performance metrics
                    perf = result.performance_analysis
                    f.write("**Performance Metrics:**\n")
                    f.write(
                        f"- CPU Utilization: {perf.avg_cpu_utilization or 'N/A'}%\n"
                    )
                    f.write(
                        f"- Memory Utilization: {perf.avg_memory_utilization or 'N/A'}%\n"
                    )
                    f.write(
                        f"- Efficiency Score: {perf.efficiency_score or 'N/A'}/100\n"
                    )
                    f.write(
                        f"- Data Processed: {perf.data_processed_gb or 'N/A'} GB\n\n"
                    )

                    # Code issues
                    code = result.code_analysis
                    if code.performance_issues or code.security_issues:
                        f.write("**Code Issues:**\n")
                        for issue in code.performance_issues or []:
                            f.write(f"- ⚠️ Performance: {issue}\n")
                        for issue in code.security_issues or []:
                            f.write(f"- 🔒 Security: {issue}\n")
                        f.write("\n")

                    f.write("---\n\n")

            self.logger.debug(f"Recommendations report saved: {md_path}")
        except Exception as e:
            self.logger.error(f"Failed to save recommendations report: {e}")
            raise

    def _save_executive_summary(self, summary: Dict[str, Any], timestamp: str):
        """Save executive summary in Markdown format"""
        md_path = self.reports_dir / f"executive_summary_{timestamp}.md"
        try:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write("# Executive Summary - Deep Analysis\n\n")
                f.write(f"**Date:** {datetime.now().strftime('%d/%m/%Y')}\n\n")

                # Key metrics
                f.write("## Key Findings\n\n")
                f.write(
                    f"- **Total Estimated Savings:** R$ {summary['total_estimated_savings_brl']:,.2f}/month\n"
                )
                f.write(f"- **Jobs Analyzed:** {summary['total_jobs_analyzed']}\n")
                f.write(f"- **High Priority Jobs:** {summary['high_priority_jobs']}\n")
                f.write(
                    f"- **Critical Issues:** {summary['critical_issues_count']}\n\n"
                )

                # Top recommendations
                f.write("## Immediate Action Items\n\n")
                for i, rec in enumerate(summary["top_recommendations"][:5], 1):
                    f.write(f"{i}. **{rec['job_name']}:** {rec['opportunity']}\n")
                f.write("\n")

                # Critical issues
                if summary["critical_issues"]:
                    f.write("## Critical Issues Requiring Attention\n\n")
                    for issue in summary["critical_issues"]:
                        f.write(f"- **{issue['job_name']}:** {issue['risk']}\n")
                    f.write("\n")

                # Analysis stats
                stats = summary.get("analysis_summary", {})
                f.write("## Analysis Statistics\n\n")
                f.write(
                    f"- Average Analysis Duration: {stats.get('avg_analysis_duration', 0):.2f} seconds\n"
                )
                f.write(
                    f"- Jobs with Code Issues: {stats.get('jobs_with_code_issues', 0)}\n"
                )
                f.write(
                    f"- Jobs with Security Issues: {stats.get('jobs_with_security_issues', 0)}\n"
                )

            self.logger.debug(f"Executive summary saved: {md_path}")
        except Exception as e:
            self.logger.error(f"Failed to save executive summary: {e}")
            raise

    def _extract_savings_from_text(self, text: str) -> float:
        """Extract savings amount from recommendation text"""
        import re

        match = re.search(r"R\$\s*([\d,]+\.?\d*)", text)
        if match:
            return float(match.group(1).replace(",", ""))
        return 0

    # Também precisa salvar os candidatos em JSON no inventory
    # Adicionar este método também:

    def _save_candidates_json(self, candidates: List[Dict], timestamp: str):
        """Save candidates in JSON format for deep analysis"""
        json_path = self.reports_dir / f"deep_analysis_candidates_{timestamp}.json"
        try:
            candidates_data = {
                "analysis_type": "candidates_for_deep_analysis",
                "timestamp": datetime.now().isoformat(),
                "candidates_count": len(candidates),
                "candidates": candidates,
            }

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(candidates_data, f, indent=2, default=str, ensure_ascii=False)

            self.logger.debug(f"Candidates JSON saved: {json_path}")
            self.logger.info(f"Candidates file for deep analysis: {json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save candidates JSON: {e}")
            raise
