from typing import Dict, List, Any
from ..core.models import CostEstimate


class CostCalculator:

    WORKER_COSTS = {
        "Standard": 0.44,
        "G.1X": 0.44,
        "G.2X": 0.88,
        "G.4X": 1.76,
        "G.8X": 3.52,
        "Z.2X": 1.00,
    }

    USD_TO_BRL = 5.2  # Pode vir de config

    @classmethod
    def quick_cost_estimate(
        cls, job_details: Dict, job_runs: List[Dict]
    ) -> CostEstimate:
        """Estimativa rápida de custos sem análise profunda"""
        worker_type = job_details.get("WorkerType", "Standard")
        num_workers = job_details.get("NumberOfWorkers", 2)
        max_capacity = job_details.get("MaxCapacity", num_workers)
        capacity = max_capacity or num_workers

        hourly_cost = cls.WORKER_COSTS.get(worker_type, 0.44) * capacity

        # Estimativa baseada em runs recentes
        if job_runs:
            execution_times = [
                run.get("ExecutionTime", 0)
                for run in job_runs
                if run.get("ExecutionTime")
            ]
            if execution_times:
                avg_hours = sum(execution_times) / len(execution_times) / 3600
                monthly_cost = hourly_cost * avg_hours * 30
            else:
                monthly_cost = 0
        else:
            monthly_cost = 0

        return CostEstimate(
            hourly_cost_usd=hourly_cost,
            estimated_monthly_usd=monthly_cost,
            estimated_monthly_brl=monthly_cost * cls.USD_TO_BRL,
        )
