from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum


class JobCategory(Enum):
    ACTIVE = "ACTIVE"
    RECENT = "RECENT"
    INACTIVE = "INACTIVE"
    ABANDONED = "ABANDONED"
    NEVER_RUN = "NEVER_RUN"


class Priority(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class JobConfig:
    glue_version: Optional[str] = None
    worker_type: Optional[str] = None
    number_of_workers: Optional[int] = None
    timeout: Optional[int] = None
    max_retries: Optional[int] = None
    description: Optional[str] = None
    job_mode: Optional[str] = None
    job_type: Optional[str] = None
    execution_class: Optional[str] = None
    script_type: Optional[str] = None
    script_location: Optional[str] = None
    avg_execution_time_minutes: Optional[float] = None
    max_capacity: Optional[float] = None
    allocated_capacity: Optional[int] = None


@dataclass
class IdleAnalysis:
    category: JobCategory
    days_idle: int
    priority: Priority
    last_run_status: Optional[str] = None


@dataclass
class CostEstimate:
    hourly_cost_usd: float
    estimated_monthly_usd: float
    estimated_monthly_brl: float


@dataclass
class TagsInfo:
    environment: str = "unknown"
    team: str = "unknown"
    business_domain: str = "unknown"
    criticality: str = "unknown"
    owner: str = "unknown"


@dataclass
class QuickCodeAnalysis:
    has_script: bool
    script_location: str
    inferred_purpose: str
    naming_issues: List[str]


@dataclass
class JobAnalysisResult:
    job_name: str
    timestamp: Optional[datetime]
    job_config: JobConfig
    idle_analysis: IdleAnalysis
    cost_estimate: CostEstimate
    tags_info: TagsInfo
    code_analysis: QuickCodeAnalysis
    recent_runs_count: int
    candidate_for_deep_analysis: Dict[str, bool]


@dataclass
class CodeAnalysis:
    script_content: Optional[str] = None
    script_size_kb: Optional[float] = None
    complexity_score: Optional[int] = None
    dependencies: List[str] = None
    sql_queries_count: Optional[int] = None
    spark_operations: List[str] = None
    performance_issues: List[str] = None
    security_issues: List[str] = None
    best_practices_violations: List[str] = None


@dataclass
class PerformanceAnalysis:
    avg_cpu_utilization: Optional[float] = None
    avg_memory_utilization: Optional[float] = None
    data_processed_gb: Optional[float] = None
    cost_per_gb: Optional[float] = None
    efficiency_score: Optional[float] = None
    bottlenecks: List[str] = None
    optimization_opportunities: List[str] = None


@dataclass
class DependencyAnalysis:
    input_sources: List[str] = None
    output_destinations: List[str] = None
    upstream_jobs: List[str] = None
    downstream_jobs: List[str] = None
    schedule_conflicts: List[str] = None
    data_lineage: Dict[str, Any] = None


@dataclass
class AIRecommendations:
    optimization_suggestions: List[str] = None
    cost_reduction_opportunities: List[str] = None
    modernization_recommendations: List[str] = None
    risk_assessment: str = None
    priority_score: Optional[float] = None
    estimated_savings_brl: Optional[float] = None


@dataclass
class DeepAnalysisResult:
    job_name: str
    timestamp: datetime
    preliminary_result: JobAnalysisResult
    code_analysis: CodeAnalysis
    performance_analysis: PerformanceAnalysis
    dependency_analysis: DependencyAnalysis
    ai_recommendations: AIRecommendations
    analysis_duration_seconds: Optional[float] = None
