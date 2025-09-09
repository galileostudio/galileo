import concurrent.futures
from datetime import datetime
from typing import List, Optional, Dict, Any
from ...core.models import JobAnalysisResult
from ...providers.aws.glue import GlueProvider
from .categorizer import JobCategorizer
from ...reporting.cost_calculator import CostCalculator
from ...utils.aws_utils import extract_tags_info
from ..static.quick_analyzer import QuickCodeAnalyzer
from ...utils.logger import setup_logger
from ...core.models import JobConfig


class InventoryScanner:

    def __init__(self, provider: GlueProvider):
        self.provider = provider
        self.categorizer = JobCategorizer()
        self.cost_calculator = CostCalculator()
        self.code_analyzer = QuickCodeAnalyzer()
        self.logger = setup_logger("galileo.inventory")

    def analyze_job_preliminary(self, job_name: str) -> JobAnalysisResult:
        """Preliminary analysis of a job"""
        self.logger.debug(f"Starting preliminary analysis for job: {job_name}")

        job_details = self.provider.get_job_details(job_name)
        if not job_details:
            self.logger.error(f"Could not fetch job details for {job_name}")
            raise Exception(f"Could not fetch job details for {job_name}")

        job_runs = self.provider.get_recent_runs(job_name)
        self.logger.debug(f"Retrieved {len(job_runs)} recent runs for job: {job_name}")

        # Calculate average execution time
        job_config = self._extract_job_config(job_details)
        job_config.avg_execution_time_minutes = self._calculate_avg_execution_time(
            job_runs
        )

        # Independent analyses
        idle_analysis = self.categorizer.categorize_by_idle_time(job_details, job_runs)
        cost_estimate = self.cost_calculator.quick_cost_estimate(job_details, job_runs)
        tags_info = extract_tags_info(job_details)
        code_analysis = self.code_analyzer.quick_code_analysis(job_details)

        # Decision about deep analysis
        deep_analysis_reasons = self.categorizer.should_analyze_deeply(
            job_details,
            job_runs,
            cost_estimate,
            idle_analysis,
            code_analysis,
            tags_info,
        )

        self.logger.info(
            f"Completed preliminary analysis for {job_name}: {idle_analysis.category.value}"
        )

        return JobAnalysisResult(
            job_name=job_name,
            timestamp=datetime.now(),
            job_config=job_config,  # Now includes avg execution time
            idle_analysis=idle_analysis,
            cost_estimate=cost_estimate,
            tags_info=tags_info,
            code_analysis=code_analysis,
            recent_runs_count=len(job_runs),
            candidate_for_deep_analysis=deep_analysis_reasons,
        )

    def _calculate_avg_execution_time(
        self, job_runs: List[Dict[str, Any]]
    ) -> Optional[float]:
        """Calculate average execution time in minutes from recent runs"""
        if not job_runs:
            return None

        # Get execution times from successful runs only
        execution_times = []
        for run in job_runs:
            if run.get("JobRunState") == "SUCCEEDED" and run.get("ExecutionTime"):
                execution_times.append(run["ExecutionTime"])

        if not execution_times:
            return None

        # Convert from seconds to minutes and return average
        avg_seconds = sum(execution_times) / len(execution_times)
        return round(avg_seconds / 60, 2)

    def _extract_job_config(self, job_details: Dict[str, Any]) -> JobConfig:
        """Extract comprehensive job configuration from job details"""
        # Determine job type from Command.Name
        command = job_details.get("Command", {})
        job_type = command.get("Name", "unknown")  # glueetl, pythonshell, gluestreaming

        # Determine script type (script vs notebook)
        script_location = command.get("ScriptLocation", "unknown")
        script_type = "notebook" if script_location.endswith(".ipynb") else "script"

        return JobConfig(
            glue_version=job_details.get("GlueVersion"),
            worker_type=job_details.get("WorkerType"),
            number_of_workers=job_details.get("NumberOfWorkers"),
            timeout=job_details.get("Timeout"),
            max_retries=job_details.get("MaxRetries"),
            max_capacity=job_details.get("MaxCapacity"),
            allocated_capacity=job_details.get("AllocatedCapacity"),
            description=job_details.get("Description", "").strip() or None,
            job_mode=job_details.get("JobMode"),
            job_type=job_type,
            execution_class=job_details.get("ExecutionClass"),
            script_type=script_type,
            script_location=script_location,
        )

    def scan_jobs(
        self, job_names: Optional[List[str]] = None
    ) -> List[JobAnalysisResult]:
        """Execute parallel scan"""
        if job_names is None:
            job_names = self.provider.get_all_jobs()

        self.logger.info(
            f"Starting scan of {len(job_names)} jobs (preliminary analysis)..."
        )

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_job = {
                executor.submit(self.analyze_job_preliminary, job_name): job_name
                for job_name in job_names
            }

            for future in concurrent.futures.as_completed(future_to_job):
                try:
                    result = future.result()
                    results.append(result)

                    category = result.idle_analysis.category.value
                    cost = result.cost_estimate.estimated_monthly_brl
                    self.logger.debug(
                        f"  {result.job_name}: {category} | R$ {cost:.2f}/month"
                    )
                except Exception as e:
                    job_name = future_to_job[future]
                    self.logger.error(f"Error analyzing {job_name}: {e}")

        self.logger.info(
            f"Scan completed. Successfully analyzed {len(results)} out of {len(job_names)} jobs"
        )
        return results
