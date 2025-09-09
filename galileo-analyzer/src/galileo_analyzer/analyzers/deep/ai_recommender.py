import asyncio
from typing import List, Dict, Any, Optional
from ...core.models import (
    JobAnalysisResult,
    CodeAnalysis,
    PerformanceAnalysis,
    DependencyAnalysis,
    AIRecommendations,
)
from ...utils.logger import setup_logger


class AIRecommender:
    """Gera recomendações inteligentes baseadas em todas as análises"""

    def __init__(self):
        self.logger = setup_logger("galileo.deep.ai")

        # Benchmarks para comparação
        self.benchmarks = {
            "cpu_utilization_optimal": (50, 80),
            "memory_utilization_optimal": (60, 85),
            "cost_per_gb_threshold": 5.0,  # R$ per GB
            "execution_time_variance_threshold": 30,  # %
        }

    async def generate_recommendations_async(
        self,
        preliminary_result: JobAnalysisResult,
        code_analysis: CodeAnalysis,
        performance_analysis: PerformanceAnalysis,
        dependency_analysis: DependencyAnalysis,
    ) -> AIRecommendations:
        """Gera recomendações baseadas em todas as análises"""

        job_name = preliminary_result.job_name
        self.logger.debug(f"Generating AI recommendations for {job_name}")

        try:
            # Coletar todas as recomendações
            cost_recommendations = self._analyze_cost_optimization(
                preliminary_result, performance_analysis
            )
            performance_recommendations = self._analyze_performance_optimization(
                code_analysis, performance_analysis
            )
            architecture_recommendations = self._analyze_architecture_improvements(
                dependency_analysis, preliminary_result
            )
            risk_assessment = self._assess_risks(
                preliminary_result, code_analysis, dependency_analysis
            )

            # Combinar e priorizar recomendações
            all_recommendations = (
                cost_recommendations
                + performance_recommendations
                + architecture_recommendations
            )

            # Calcular score de prioridade
            priority_score = self._calculate_priority_score(
                preliminary_result, performance_analysis
            )

            # Estimar economia total
            estimated_savings = self._estimate_total_savings(all_recommendations)

            return AIRecommendations(
                optimization_suggestions=self._format_recommendations(
                    all_recommendations
                ),
                cost_reduction_opportunities=self._extract_cost_opportunities(
                    all_recommendations
                ),
                modernization_recommendations=self._extract_modernization_opportunities(
                    all_recommendations
                ),
                risk_assessment=risk_assessment,
                priority_score=priority_score,
                estimated_savings_brl=estimated_savings,
            )

        except Exception as e:
            self.logger.error(f"Failed to generate recommendations for {job_name}: {e}")
            return AIRecommendations()

    def _analyze_cost_optimization(
        self,
        preliminary_result: JobAnalysisResult,
        performance_analysis: PerformanceAnalysis,
    ) -> List[Dict[str, Any]]:
        """Analisa oportunidades de otimização de custo"""
        recommendations = []

        job_config = preliminary_result.job_config
        cost_estimate = preliminary_result.cost_estimate

        # Análise de worker type
        worker_type = job_config.worker_type or "Standard"
        cpu_util = performance_analysis.avg_cpu_utilization
        memory_util = performance_analysis.avg_memory_utilization

        if cpu_util and memory_util:
            if cpu_util < 30 and memory_util < 30:
                if worker_type in ["G.4X", "G.8X"]:
                    potential_savings = cost_estimate.estimated_monthly_brl * 0.5
                    recommendations.append(
                        {
                            "type": "COST_OPTIMIZATION",
                            "category": "worker_downgrade",
                            "title": f"Downgrade worker type from {worker_type}",
                            "description": f"CPU ({cpu_util}%) and memory ({memory_util}%) utilization are low",
                            "recommendation": f"Consider downgrading to G.2X or G.1X workers",
                            "estimated_savings_monthly": potential_savings,
                            "implementation_effort": "LOW",
                            "confidence": "HIGH",
                        }
                    )
                elif worker_type == "G.2X":
                    potential_savings = cost_estimate.estimated_monthly_brl * 0.3
                    recommendations.append(
                        {
                            "type": "COST_OPTIMIZATION",
                            "category": "worker_downgrade",
                            "title": "Downgrade to G.1X workers",
                            "description": f"Low resource utilization detected",
                            "recommendation": "Downgrade to G.1X workers for better cost efficiency",
                            "estimated_savings_monthly": potential_savings,
                            "implementation_effort": "LOW",
                            "confidence": "MEDIUM",
                        }
                    )

        # Análise de número de workers
        num_workers = job_config.number_of_workers
        if num_workers and num_workers > 5 and cpu_util and cpu_util < 40:
            potential_savings = cost_estimate.estimated_monthly_brl * 0.2
            recommendations.append(
                {
                    "type": "COST_OPTIMIZATION",
                    "category": "worker_count",
                    "title": f"Reduce number of workers from {num_workers}",
                    "description": "Low CPU utilization suggests over-provisioning",
                    "recommendation": f"Consider reducing to {max(2, num_workers // 2)} workers",
                    "estimated_savings_monthly": potential_savings,
                    "implementation_effort": "LOW",
                    "confidence": "MEDIUM",
                }
            )

        # Jobs nunca executados
        if preliminary_result.idle_analysis.category.value == "NEVER_RUN":
            recommendations.append(
                {
                    "type": "COST_OPTIMIZATION",
                    "category": "job_removal",
                    "title": "Remove unused job",
                    "description": "Job has never been executed",
                    "recommendation": "Consider removing this job if no longer needed",
                    "estimated_savings_monthly": cost_estimate.estimated_monthly_brl,
                    "implementation_effort": "LOW",
                    "confidence": "HIGH",
                }
            )

        # Jobs abandonados
        elif preliminary_result.idle_analysis.category.value == "ABANDONED":
            days_idle = preliminary_result.idle_analysis.days_idle
            recommendations.append(
                {
                    "type": "COST_OPTIMIZATION",
                    "category": "job_review",
                    "title": "Review abandoned job",
                    "description": f"Job has been idle for {days_idle} days",
                    "recommendation": "Review if job is still needed or can be archived",
                    "estimated_savings_monthly": cost_estimate.estimated_monthly_brl,
                    "implementation_effort": "MEDIUM",
                    "confidence": "HIGH",
                }
            )

        return recommendations

    def _analyze_performance_optimization(
        self, code_analysis: CodeAnalysis, performance_analysis: PerformanceAnalysis
    ) -> List[Dict[str, Any]]:
        """Analisa oportunidades de otimização de performance"""
        recommendations = []

        # Análise de problemas no código
        if code_analysis.performance_issues:
            for issue in code_analysis.performance_issues:
                if "collect()" in issue:
                    recommendations.append(
                        {
                            "type": "PERFORMANCE",
                            "category": "code_optimization",
                            "title": "Remove collect() operations",
                            "description": "collect() brings all data to driver, causing memory issues",
                            "recommendation": "Replace collect() with write operations or use take() with limit",
                            "estimated_savings_monthly": 200,  # Savings from avoiding failures
                            "implementation_effort": "MEDIUM",
                            "confidence": "HIGH",
                        }
                    )

                elif "count() in loop" in issue:
                    recommendations.append(
                        {
                            "type": "PERFORMANCE",
                            "category": "code_optimization",
                            "title": "Optimize count() operations",
                            "description": "count() in loops is inefficient",
                            "recommendation": "Cache DataFrames before loops or restructure logic",
                            "estimated_savings_monthly": 150,
                            "implementation_effort": "MEDIUM",
                            "confidence": "HIGH",
                        }
                    )

        # Análise de eficiência
        efficiency_score = performance_analysis.efficiency_score
        if efficiency_score and efficiency_score < 50:
            recommendations.append(
                {
                    "type": "PERFORMANCE",
                    "category": "resource_optimization",
                    "title": f"Improve resource efficiency (current: {efficiency_score}%)",
                    "description": "Low resource utilization detected",
                    "recommendation": "Review job configuration and optimize resource allocation",
                    "estimated_savings_monthly": 300,
                    "implementation_effort": "HIGH",
                    "confidence": "MEDIUM",
                }
            )

        # Análise de bottlenecks
        if performance_analysis.bottlenecks:
            for bottleneck in performance_analysis.bottlenecks:
                recommendations.append(
                    {
                        "type": "PERFORMANCE",
                        "category": "bottleneck_resolution",
                        "title": "Resolve performance bottleneck",
                        "description": bottleneck,
                        "recommendation": self._get_bottleneck_recommendation(
                            bottleneck
                        ),
                        "estimated_savings_monthly": 100,
                        "implementation_effort": "MEDIUM",
                        "confidence": "MEDIUM",
                    }
                )

        return recommendations

    def _analyze_architecture_improvements(
        self,
        dependency_analysis: DependencyAnalysis,
        preliminary_result: JobAnalysisResult,
    ) -> List[Dict[str, Any]]:
        """Analisa melhorias de arquitetura"""
        recommendations = []

        # Análise de complexidade de dependências
        input_count = len(dependency_analysis.input_sources or [])
        output_count = len(dependency_analysis.output_destinations or [])

        if input_count > 5:
            recommendations.append(
                {
                    "type": "ARCHITECTURE",
                    "category": "dependency_simplification",
                    "title": f"Simplify data dependencies ({input_count} inputs)",
                    "description": "High number of input sources increases complexity",
                    "recommendation": "Consider consolidating data sources or breaking job into smaller pieces",
                    "estimated_savings_monthly": 0,
                    "implementation_effort": "HIGH",
                    "confidence": "MEDIUM",
                }
            )

        # Análise de conflitos de schedule
        if dependency_analysis.schedule_conflicts:
            for conflict in dependency_analysis.schedule_conflicts:
                recommendations.append(
                    {
                        "type": "ARCHITECTURE",
                        "category": "schedule_optimization",
                        "title": "Resolve schedule conflict",
                        "description": conflict,
                        "recommendation": "Review and optimize job scheduling to avoid conflicts",
                        "estimated_savings_monthly": 50,
                        "implementation_effort": "MEDIUM",
                        "confidence": "HIGH",
                    }
                )

        # Recomendações de modernização
        job_config = preliminary_result.job_config
        if job_config.glue_version and float(job_config.glue_version) < 3.0:
            recommendations.append(
                {
                    "type": "MODERNIZATION",
                    "category": "version_upgrade",
                    "title": f"Upgrade Glue version from {job_config.glue_version}",
                    "description": "Older Glue versions have performance limitations",
                    "recommendation": "Upgrade to Glue 3.0 or higher for better performance",
                    "estimated_savings_monthly": 100,
                    "implementation_effort": "MEDIUM",
                    "confidence": "HIGH",
                }
            )

        return recommendations

    def _assess_risks(
        self,
        preliminary_result: JobAnalysisResult,
        code_analysis: CodeAnalysis,
        dependency_analysis: DependencyAnalysis,
    ) -> str:
        """Avalia riscos do job"""
        risks = []

        # Riscos de segurança
        if code_analysis.security_issues:
            risks.append("SECURITY: Potential security vulnerabilities in code")

        # Riscos de performance
        if code_analysis.performance_issues:
            risks.append("PERFORMANCE: Code patterns that may cause failures")

        # Riscos de dependência
        if len(dependency_analysis.input_sources or []) > 10:
            risks.append("DEPENDENCY: High complexity due to many dependencies")

        # Riscos de custo
        cost = preliminary_result.cost_estimate.estimated_monthly_brl
        if cost > 1000:
            risks.append(f"COST: High monthly cost (R$ {cost:.2f})")

        # Riscos operacionais
        if preliminary_result.idle_analysis.category.value in [
            "ABANDONED",
            "NEVER_RUN",
        ]:
            risks.append("OPERATIONAL: Job may be obsolete")

        if not risks:
            return "LOW: No significant risks identified"
        elif len(risks) <= 2:
            return f"MEDIUM: {'; '.join(risks)}"
        else:
            return f"HIGH: Multiple risks identified - {'; '.join(risks[:3])}"

    def _calculate_priority_score(
        self,
        preliminary_result: JobAnalysisResult,
        performance_analysis: PerformanceAnalysis,
    ) -> float:
        """Calcula score de prioridade (0-100)"""
        score = 0

        # Factor 1: Custo (40% do score)
        cost = preliminary_result.cost_estimate.estimated_monthly_brl
        if cost > 2000:
            score += 40
        elif cost > 1000:
            score += 30
        elif cost > 500:
            score += 20
        elif cost > 100:
            score += 10

        # Factor 2: Eficiência (30% do score)
        efficiency = performance_analysis.efficiency_score
        if efficiency and efficiency < 30:
            score += 30
        elif efficiency and efficiency < 50:
            score += 20
        elif efficiency and efficiency < 70:
            score += 10

        # Factor 3: Status do job (20% do score)
        category = preliminary_result.idle_analysis.category.value
        if category == "NEVER_RUN":
            score += 20
        elif category == "ABANDONED":
            score += 15
        elif category == "INACTIVE":
            score += 10

        # Factor 4: Problemas de código (10% do score)
        if preliminary_result.code_analysis.naming_issues:
            score += 5

        return min(100, score)

    def _estimate_total_savings(self, recommendations: List[Dict[str, Any]]) -> float:
        """Estima economia total das recomendações"""
        total_savings = 0

        for rec in recommendations:
            savings = rec.get("estimated_savings_monthly", 0)
            confidence = rec.get("confidence", "LOW")

            # Aplica fator de confiança
            if confidence == "HIGH":
                total_savings += savings * 0.9
            elif confidence == "MEDIUM":
                total_savings += savings * 0.7
            else:
                total_savings += savings * 0.5

        return round(total_savings, 2)

    def _format_recommendations(
        self, recommendations: List[Dict[str, Any]]
    ) -> List[str]:
        """Formata recomendações para exibição"""
        formatted = []

        # Ordena por economia potencial
        sorted_recs = sorted(
            recommendations,
            key=lambda x: x.get("estimated_savings_monthly", 0),
            reverse=True,
        )

        for rec in sorted_recs[:10]:  # Top 10
            savings = rec.get("estimated_savings_monthly", 0)
            effort = rec.get("implementation_effort", "UNKNOWN")

            if savings > 0:
                formatted.append(
                    f"{rec['title']} - Save R$ {savings:.2f}/month ({effort} effort)"
                )
            else:
                formatted.append(f"{rec['title']} ({effort} effort)")

        return formatted

    def _extract_cost_opportunities(
        self, recommendations: List[Dict[str, Any]]
    ) -> List[str]:
        """Extrai apenas oportunidades de redução de custo"""
        cost_ops = []

        for rec in recommendations:
            if (
                rec["type"] == "COST_OPTIMIZATION"
                and rec.get("estimated_savings_monthly", 0) > 0
            ):
                savings = rec["estimated_savings_monthly"]
                cost_ops.append(f"{rec['title']} - R$ {savings:.2f}/month")

        return cost_ops

    def _extract_modernization_opportunities(
        self, recommendations: List[Dict[str, Any]]
    ) -> List[str]:
        """Extrai oportunidades de modernização"""
        modern_ops = []

        for rec in recommendations:
            if rec["type"] in ["MODERNIZATION", "ARCHITECTURE"]:
                modern_ops.append(rec["title"])

        return modern_ops

    def _get_bottleneck_recommendation(self, bottleneck: str) -> str:
        """Gera recomendação específica para bottleneck"""
        if "CPU under-utilized" in bottleneck:
            return "Reduce instance type or number of workers"
        elif "Memory under-utilized" in bottleneck:
            return "Switch to memory-optimized instance or reduce allocation"
        elif "CPU over-utilized" in bottleneck:
            return "Increase instance type or add more workers"
        elif "Memory over-utilized" in bottleneck:
            return "Increase memory allocation or optimize code"
        elif "execution time variability" in bottleneck:
            return "Investigate data skew and optimize partitioning"
        else:
            return "Review job configuration and optimization opportunities"
