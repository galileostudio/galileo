import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from ...core.models import JobAnalysisResult, PerformanceAnalysis
from ...providers.aws.glue import GlueProvider
from ...utils.logger import setup_logger


class PerformanceAnalyzer:
    """Analisa performance detalhada dos jobs usando CloudWatch"""

    def __init__(self, provider: GlueProvider):
        self.provider = provider
        self.cloudwatch = provider.cloudwatch
        self.logger = setup_logger("galileo.deep.performance")

    async def analyze_performance_async(
        self, preliminary_result: JobAnalysisResult
    ) -> PerformanceAnalysis:
        """Análise assíncrona de performance"""
        job_name = preliminary_result.job_name

        self.logger.debug(f"Starting performance analysis for {job_name}")

        try:
            # Período de análise: últimos 30 dias
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)

            # Coletar métricas em paralelo
            tasks = [
                self._get_cpu_utilization(job_name, start_time, end_time),
                self._get_memory_utilization(job_name, start_time, end_time),
                self._calculate_data_processed(preliminary_result),
                self._analyze_execution_patterns(preliminary_result),
            ]

            cpu_util, memory_util, data_stats, exec_patterns = await asyncio.gather(
                *tasks
            )

            # Calcular métricas derivadas
            efficiency_score = self._calculate_efficiency_score(cpu_util, memory_util)
            cost_per_gb = self._calculate_cost_per_gb(preliminary_result, data_stats)
            bottlenecks = self._identify_bottlenecks(
                cpu_util, memory_util, exec_patterns
            )
            optimizations = self._suggest_optimizations(
                preliminary_result, cpu_util, memory_util, cost_per_gb
            )

            return PerformanceAnalysis(
                avg_cpu_utilization=cpu_util,
                avg_memory_utilization=memory_util,
                data_processed_gb=data_stats.get("total_gb", 0),
                cost_per_gb=cost_per_gb,
                efficiency_score=efficiency_score,
                bottlenecks=bottlenecks,
                optimization_opportunities=optimizations,
            )

        except Exception as e:
            self.logger.error(f"Performance analysis failed for {job_name}: {e}")
            return PerformanceAnalysis()

    async def _get_cpu_utilization(
        self, job_name: str, start_time: datetime, end_time: datetime
    ) -> Optional[float]:
        """Obtém utilização média de CPU do CloudWatch"""
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/Glue",
                MetricName="glue.driver.aggregate.numCompletedTasks",
                Dimensions=[{"Name": "JobName", "Value": job_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hora
                Statistics=["Average"],
            )

            if response["Datapoints"]:
                avg_cpu = sum(
                    point["Average"] for point in response["Datapoints"]
                ) / len(response["Datapoints"])
                return round(avg_cpu, 2)

            return None

        except Exception as e:
            self.logger.warning(f"Could not get CPU metrics for {job_name}: {e}")
            return None

    async def _get_memory_utilization(
        self, job_name: str, start_time: datetime, end_time: datetime
    ) -> Optional[float]:
        """Obtém utilização média de memória"""
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/Glue",
                MetricName="glue.driver.jvm.heap.usage",
                Dimensions=[{"Name": "JobName", "Value": job_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=["Average"],
            )

            if response["Datapoints"]:
                avg_memory = sum(
                    point["Average"] for point in response["Datapoints"]
                ) / len(response["Datapoints"])
                return round(avg_memory * 100, 2)  # Convert to percentage

            return None

        except Exception as e:
            self.logger.warning(f"Could not get memory metrics for {job_name}: {e}")
            return None

    async def _calculate_data_processed(
        self, preliminary_result: JobAnalysisResult
    ) -> Dict[str, float]:
        """Estima dados processados baseado nos runs"""
        try:
            job_runs = await self._get_detailed_job_runs(preliminary_result.job_name)

            total_bytes = 0
            successful_runs = [
                run for run in job_runs if run.get("JobRunState") == "SUCCEEDED"
            ]

            for run in successful_runs:
                # Estimativa baseada em tempo de execução e tipo de worker
                execution_time = run.get("ExecutionTime", 0)
                worker_type = preliminary_result.job_config.worker_type or "Standard"

                # Heurística: dados processados baseado em tempo e tipo de worker
                if worker_type == "G.1X":
                    bytes_per_second = 10 * 1024 * 1024  # 10MB/s
                elif worker_type == "G.2X":
                    bytes_per_second = 20 * 1024 * 1024  # 20MB/s
                elif worker_type == "G.4X":
                    bytes_per_second = 40 * 1024 * 1024  # 40MB/s
                else:
                    bytes_per_second = 5 * 1024 * 1024  # 5MB/s default

                estimated_bytes = execution_time * bytes_per_second
                total_bytes += estimated_bytes

            total_gb = total_bytes / (1024**3) if total_bytes > 0 else 0

            return {
                "total_gb": round(total_gb, 2),
                "runs_analyzed": len(successful_runs),
            }

        except Exception as e:
            self.logger.warning(f"Could not calculate data processed: {e}")
            return {"total_gb": 0, "runs_analyzed": 0}

    async def _get_detailed_job_runs(self, job_name: str) -> List[Dict]:
        """Obtém runs detalhados do job"""
        try:
            return self.provider.get_recent_runs(job_name, max_results=20)
        except Exception as e:
            self.logger.warning(f"Could not get detailed runs for {job_name}: {e}")
            return []

    async def _analyze_execution_patterns(
        self, preliminary_result: JobAnalysisResult
    ) -> Dict[str, Any]:
        """Analisa padrões de execução"""
        try:
            job_runs = await self._get_detailed_job_runs(preliminary_result.job_name)

            if not job_runs:
                return {}

            execution_times = [
                run.get("ExecutionTime", 0)
                for run in job_runs
                if run.get("ExecutionTime")
            ]

            if not execution_times:
                return {}

            avg_time = sum(execution_times) / len(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)

            # Calcular variabilidade
            variance = sum((t - avg_time) ** 2 for t in execution_times) / len(
                execution_times
            )
            std_dev = variance**0.5

            return {
                "avg_execution_time": round(avg_time, 2),
                "min_execution_time": min_time,
                "max_execution_time": max_time,
                "execution_variability": (
                    round(std_dev / avg_time * 100, 2) if avg_time > 0 else 0
                ),
                "total_runs_analyzed": len(execution_times),
            }

        except Exception as e:
            self.logger.warning(f"Could not analyze execution patterns: {e}")
            return {}

    def _calculate_efficiency_score(
        self, cpu_util: Optional[float], memory_util: Optional[float]
    ) -> Optional[float]:
        """Calcula score de eficiência (0-100)"""
        if cpu_util is None and memory_util is None:
            return None

        # Score baseado em utilização de recursos
        cpu_score = min(cpu_util / 80 * 100, 100) if cpu_util else 50
        memory_score = min(memory_util / 80 * 100, 100) if memory_util else 50

        # Penaliza sub-utilização
        if cpu_util and cpu_util < 20:
            cpu_score *= 0.5
        if memory_util and memory_util < 20:
            memory_score *= 0.5

        efficiency = (cpu_score + memory_score) / 2
        return round(efficiency, 1)

    def _calculate_cost_per_gb(
        self, preliminary_result: JobAnalysisResult, data_stats: Dict
    ) -> Optional[float]:
        """Calcula custo por GB processado"""
        try:
            monthly_cost = preliminary_result.cost_estimate.estimated_monthly_brl
            total_gb = data_stats.get("total_gb", 0)

            if total_gb > 0:
                return round(monthly_cost / total_gb, 2)

            return None

        except Exception:
            return None

    def _identify_bottlenecks(
        self,
        cpu_util: Optional[float],
        memory_util: Optional[float],
        exec_patterns: Dict,
    ) -> List[str]:
        """Identifica gargalos de performance"""
        bottlenecks = []

        if cpu_util and cpu_util < 20:
            bottlenecks.append(
                "CPU under-utilized (< 20%) - consider smaller instance type"
            )

        if memory_util and memory_util < 20:
            bottlenecks.append("Memory under-utilized (< 20%) - over-provisioned")

        if cpu_util and cpu_util > 90:
            bottlenecks.append("CPU over-utilized (> 90%) - may need more resources")

        if memory_util and memory_util > 90:
            bottlenecks.append("Memory over-utilized (> 90%) - risk of OOM errors")

        variability = exec_patterns.get("execution_variability", 0)
        if variability > 50:
            bottlenecks.append(
                f"High execution time variability ({variability}%) - inconsistent performance"
            )

        return bottlenecks

    def _suggest_optimizations(
        self,
        preliminary_result: JobAnalysisResult,
        cpu_util: Optional[float],
        memory_util: Optional[float],
        cost_per_gb: Optional[float],
    ) -> List[str]:
        """Sugere otimizações específicas"""
        optimizations = []

        worker_type = preliminary_result.job_config.worker_type

        # Otimizações de worker type
        if cpu_util and memory_util:
            if cpu_util < 30 and memory_util < 30:
                if worker_type in ["G.4X", "G.8X"]:
                    optimizations.append(
                        "Consider downgrading to G.2X workers for better cost efficiency"
                    )
                elif worker_type == "G.2X":
                    optimizations.append("Consider downgrading to G.1X workers")

        # Otimizações de custo
        if cost_per_gb and cost_per_gb > 10:  # R$ 10 per GB is expensive
            optimizations.append("High cost per GB - review data processing efficiency")

        # Otimizações de configuração
        num_workers = preliminary_result.job_config.number_of_workers
        if num_workers and num_workers > 10 and cpu_util and cpu_util < 40:
            optimizations.append("Consider reducing number of workers")

        return optimizations
