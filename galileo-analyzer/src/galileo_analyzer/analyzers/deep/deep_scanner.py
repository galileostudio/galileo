import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from ...core.models import JobAnalysisResult, DeepAnalysisResult
from ...providers.aws.glue import GlueProvider
from .code_analyzer import DeepCodeAnalyzer
from .performance_analyzer import PerformanceAnalyzer
from .dependency_analyzer import DependencyAnalyzer
from .ai_recommender import AIRecommender
from ...utils.logger import setup_logger


class DeepScanner:
    """Scanner para análise profunda de jobs selecionados"""

    def __init__(self, provider: GlueProvider):
        self.provider = provider
        self.code_analyzer = DeepCodeAnalyzer(provider)
        self.performance_analyzer = PerformanceAnalyzer(provider)
        self.dependency_analyzer = DependencyAnalyzer(provider)
        self.ai_recommender = AIRecommender()
        self.logger = setup_logger("galileo.deep")

    async def analyze_job_deeply(
        self, preliminary_result: JobAnalysisResult
    ) -> DeepAnalysisResult:
        """Análise profunda de um job específico"""
        start_time = time.time()
        job_name = preliminary_result.job_name

        self.logger.info(f"Starting deep analysis for: {job_name}")

        # Executar análises em paralelo onde possível
        tasks = []

        # Análise de código (pode ser demorada - download do script)
        code_task = asyncio.create_task(
            self.code_analyzer.analyze_code_async(preliminary_result)
        )
        tasks.append(("code", code_task))

        # Análise de performance (pode fazer em paralelo)
        perf_task = asyncio.create_task(
            self.performance_analyzer.analyze_performance_async(preliminary_result)
        )
        tasks.append(("performance", perf_task))

        # Análise de dependências
        dep_task = asyncio.create_task(
            self.dependency_analyzer.analyze_dependencies_async(preliminary_result)
        )
        tasks.append(("dependencies", dep_task))

        # Aguardar todas as análises
        results = {}
        for name, task in tasks:
            try:
                results[name] = await task
                self.logger.debug(f"Completed {name} analysis for {job_name}")
            except Exception as e:
                self.logger.error(f"Failed {name} analysis for {job_name}: {e}")
                results[name] = self._get_empty_analysis(name)

        # Gerar recomendações com IA (usando todos os resultados)
        ai_recommendations = await self.ai_recommender.generate_recommendations_async(
            preliminary_result,
            results["code"],
            results["performance"],
            results["dependencies"],
        )

        duration = time.time() - start_time
        self.logger.info(f"Deep analysis completed for {job_name} in {duration:.2f}s")

        return DeepAnalysisResult(
            job_name=job_name,
            timestamp=datetime.now(),
            preliminary_result=preliminary_result,
            code_analysis=results["code"],
            performance_analysis=results["performance"],
            dependency_analysis=results["dependencies"],
            ai_recommendations=ai_recommendations,
            analysis_duration_seconds=duration,
        )

    def _get_empty_analysis(self, analysis_type: str):
        """Retorna análise vazia em caso de erro"""
        if analysis_type == "code":
            return CodeAnalysis()
        elif analysis_type == "performance":
            return PerformanceAnalysis()
        elif analysis_type == "dependencies":
            return DependencyAnalysis()
