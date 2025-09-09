import asyncio
import re
from typing import List, Dict, Any, Optional, Set
from ...core.models import JobAnalysisResult, DependencyAnalysis
from ...providers.aws.glue import GlueProvider
from ...utils.logger import setup_logger


class DependencyAnalyzer:
    """Analisa dependências e relacionamentos entre jobs"""

    def __init__(self, provider: GlueProvider):
        self.provider = provider
        self.s3_client = provider.s3
        self.logger = setup_logger("galileo.deep.dependencies")

    async def analyze_dependencies_async(
        self, preliminary_result: JobAnalysisResult
    ) -> DependencyAnalysis:
        """Análise assíncrona de dependências"""
        job_name = preliminary_result.job_name

        self.logger.debug(f"Starting dependency analysis for {job_name}")

        try:
            # Analisar script para extrair dependências
            script_analysis = await self._analyze_script_dependencies(
                preliminary_result
            )

            # Analisar dependências no nível do Glue
            glue_dependencies = await self._analyze_glue_dependencies(job_name)

            # Detectar conflitos de schedule
            schedule_conflicts = await self._detect_schedule_conflicts(
                preliminary_result
            )

            # Mapear lineage de dados
            data_lineage = await self._map_data_lineage(script_analysis)

            return DependencyAnalysis(
                input_sources=script_analysis.get("inputs", []),
                output_destinations=script_analysis.get("outputs", []),
                upstream_jobs=glue_dependencies.get("upstream", []),
                downstream_jobs=glue_dependencies.get("downstream", []),
                schedule_conflicts=schedule_conflicts,
                data_lineage=data_lineage,
            )

        except Exception as e:
            self.logger.error(f"Dependency analysis failed for {job_name}: {e}")
            return DependencyAnalysis()

    async def _analyze_script_dependencies(
        self, preliminary_result: JobAnalysisResult
    ) -> Dict[str, List[str]]:
        """Analisa dependências do script"""
        script_location = preliminary_result.job_config.script_location

        if not script_location or script_location == "unknown":
            return {"inputs": [], "outputs": []}

        try:
            script_content = await self._download_script(script_location)
            if not script_content:
                return {"inputs": [], "outputs": []}

            inputs = self._extract_input_sources(script_content)
            outputs = self._extract_output_destinations(script_content)

            return {"inputs": list(inputs), "outputs": list(outputs)}

        except Exception as e:
            self.logger.warning(f"Could not analyze script dependencies: {e}")
            return {"inputs": [], "outputs": []}

    async def _download_script(self, s3_path: str) -> Optional[str]:
        """Download do script do S3"""
        try:
            if not s3_path.startswith("s3://"):
                return None

            path_parts = s3_path[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""

            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            return content

        except Exception as e:
            self.logger.warning(f"Failed to download script {s3_path}: {e}")
            return None

    def _extract_input_sources(self, script_content: str) -> Set[str]:
        """Extrai fontes de dados de entrada do script"""
        inputs = set()

        # Padrões S3
        s3_patterns = [
            r's3://([a-zA-Z0-9.-]+/[^\s\'"]+)',
            r's3a://([a-zA-Z0-9.-]+/[^\s\'"]+)',
            r'"s3://([^"]+)"',
            r"'s3://([^']+)'",
        ]

        for pattern in s3_patterns:
            matches = re.findall(pattern, script_content)
            for match in matches:
                if isinstance(match, str):
                    inputs.add(f"s3://{match}")
                else:
                    inputs.add(f"s3://{match[0] if match else ''}")

        # Padrões de tabelas do Glue Catalog
        table_patterns = [
            r'\.table\(\s*["\']([^"\']+)["\']',
            r'spark\.sql\(["\'].*?FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\.read\.table\(["\']([^"\']+)["\']',
        ]

        for pattern in table_patterns:
            matches = re.findall(pattern, script_content, re.IGNORECASE)
            for match in matches:
                inputs.add(f"glue_catalog:{match}")

        # Padrões JDBC
        jdbc_patterns = [
            r'jdbc:([^"\']+)',
        ]

        for pattern in jdbc_patterns:
            matches = re.findall(pattern, script_content)
            for match in matches:
                inputs.add(f"jdbc:{match}")

        return inputs

    def _extract_output_destinations(self, script_content: str) -> Set[str]:
        """Extrai destinos de dados de saída do script"""
        outputs = set()

        # Padrões S3 para escrita
        s3_write_patterns = [
            r'\.write[^(]*\([^)]*["\']s3://([^"\']+)["\']',
            r'\.save\(["\']s3://([^"\']+)["\']',
            r'\.option\(["\']path["\'],\s*["\']s3://([^"\']+)["\']',
        ]

        for pattern in s3_write_patterns:
            matches = re.findall(pattern, script_content)
            for match in matches:
                outputs.add(f"s3://{match}")

        # Padrões de tabelas para escrita
        table_write_patterns = [
            r'\.saveAsTable\(["\']([^"\']+)["\']',
            r'\.insertInto\(["\']([^"\']+)["\']',
            r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        ]

        for pattern in table_write_patterns:
            matches = re.findall(pattern, script_content, re.IGNORECASE)
            for match in matches:
                outputs.add(f"glue_catalog:{match}")

        return outputs

    async def _analyze_glue_dependencies(self, job_name: str) -> Dict[str, List[str]]:
        """Analisa dependências no nível do AWS Glue"""
        try:
            # Buscar triggers que incluem este job
            triggers_response = self.provider.glue.get_triggers()

            upstream_jobs = []
            downstream_jobs = []

            for trigger in triggers_response.get("Triggers", []):
                actions = trigger.get("Actions", [])
                job_names_in_trigger = [
                    action.get("JobName") for action in actions if action.get("JobName")
                ]

                if job_name in job_names_in_trigger:
                    # Jobs que executam antes deste
                    predicate = trigger.get("Predicate", {})
                    conditions = predicate.get("Conditions", [])

                    for condition in conditions:
                        if condition.get("LogicalOperator") == "EQUALS":
                            upstream_job = condition.get("JobName")
                            if upstream_job and upstream_job != job_name:
                                upstream_jobs.append(upstream_job)

                # Verificar se este job dispara outros
                for action in actions:
                    if action.get("JobName") != job_name:
                        # Este job pode disparar outros jobs via triggers
                        downstream_jobs.append(action.get("JobName"))

            return {
                "upstream": list(set(upstream_jobs)),
                "downstream": list(set(downstream_jobs)),
            }

        except Exception as e:
            self.logger.warning(f"Could not analyze Glue dependencies: {e}")
            return {"upstream": [], "downstream": []}

    async def _detect_schedule_conflicts(
        self, preliminary_result: JobAnalysisResult
    ) -> List[str]:
        """Detecta conflitos de schedule"""
        try:
            job_name = preliminary_result.job_name

            # Buscar triggers deste job
            triggers_response = self.provider.glue.get_triggers()

            conflicts = []
            job_schedules = []

            for trigger in triggers_response.get("Triggers", []):
                actions = trigger.get("Actions", [])
                job_names = [
                    action.get("JobName") for action in actions if action.get("JobName")
                ]

                if job_name in job_names:
                    schedule = trigger.get("Schedule")
                    if schedule:
                        job_schedules.append(schedule)

            # Verificar sobreposição com outros jobs (simplificado)
            if len(job_schedules) > 1:
                conflicts.append("Multiple schedules found for the same job")

            # Aqui você poderia implementar lógica mais sofisticada para detectar
            # conflitos reais de horário baseado nos schedules cron

            return conflicts

        except Exception as e:
            self.logger.warning(f"Could not detect schedule conflicts: {e}")
            return []

    async def _map_data_lineage(self, script_analysis: Dict) -> Dict[str, Any]:
        """Mapeia lineage de dados"""
        try:
            inputs = script_analysis.get("inputs", [])
            outputs = script_analysis.get("outputs", [])

            lineage = {
                "input_types": self._categorize_sources(inputs),
                "output_types": self._categorize_sources(outputs),
                "transformation_complexity": self._estimate_transformation_complexity(
                    inputs, outputs
                ),
            }

            return lineage

        except Exception as e:
            self.logger.warning(f"Could not map data lineage: {e}")
            return {}

    def _categorize_sources(self, sources: List[str]) -> Dict[str, int]:
        """Categoriza tipos de fontes de dados"""
        categories = {"s3": 0, "glue_catalog": 0, "jdbc": 0, "other": 0}

        for source in sources:
            if source.startswith("s3://"):
                categories["s3"] += 1
            elif source.startswith("glue_catalog:"):
                categories["glue_catalog"] += 1
            elif source.startswith("jdbc:"):
                categories["jdbc"] += 1
            else:
                categories["other"] += 1

        return categories

    def _estimate_transformation_complexity(
        self, inputs: List[str], outputs: List[str]
    ) -> str:
        """Estima complexidade da transformação"""
        input_count = len(inputs)
        output_count = len(outputs)

        if input_count == 0 and output_count == 0:
            return "unknown"
        elif input_count == 1 and output_count == 1:
            return "simple"
        elif input_count <= 3 and output_count <= 3:
            return "moderate"
        else:
            return "complex"
