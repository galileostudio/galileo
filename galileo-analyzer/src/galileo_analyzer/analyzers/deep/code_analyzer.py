import re
import ast
import boto3
from typing import List, Optional, Dict, Any
from ...core.models import JobAnalysisResult, CodeAnalysis
from ...providers.aws.glue import GlueProvider
from ...utils.logger import setup_logger


class DeepCodeAnalyzer:
    """Análise profunda de código dos scripts"""

    def __init__(self, provider: GlueProvider):
        self.provider = provider
        self.s3_client = provider.s3
        self.logger = setup_logger("galileo.deep.code")

    async def analyze_code_async(
        self, preliminary_result: JobAnalysisResult
    ) -> CodeAnalysis:
        """Análise assíncrona do código"""
        job_config = preliminary_result.job_config
        script_location = job_config.script_location

        if not script_location or script_location == "unknown":
            self.logger.warning(f"No script location for {preliminary_result.job_name}")
            return CodeAnalysis()

        # Download do script
        script_content = await self._download_script(script_location)
        if not script_content:
            return CodeAnalysis()

        # Análises do código
        return CodeAnalysis(
            script_content=script_content[:10000],  # Primeiros 10KB para logs
            script_size_kb=len(script_content) / 1024,
            complexity_score=self._calculate_complexity(script_content),
            dependencies=self._extract_dependencies(script_content),
            sql_queries_count=self._count_sql_queries(script_content),
            spark_operations=self._extract_spark_operations(script_content),
            performance_issues=self._detect_performance_issues(script_content),
            security_issues=self._detect_security_issues(script_content),
            best_practices_violations=self._check_best_practices(script_content),
        )

    async def _download_script(self, s3_path: str) -> Optional[str]:
        """Download do script do S3"""
        try:
            # Parse S3 path: s3://bucket/key
            if not s3_path.startswith("s3://"):
                return None

            path_parts = s3_path[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""

            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            self.logger.debug(f"Downloaded script: {len(content)} chars from {s3_path}")
            return content

        except Exception as e:
            self.logger.error(f"Failed to download script {s3_path}: {e}")
            return None

    def _calculate_complexity(self, code: str) -> int:
        """Calcula complexidade ciclomática básica"""
        try:
            tree = ast.parse(code)
            complexity = 1  # Base complexity

            for node in ast.walk(tree):
                if isinstance(node, (ast.If, ast.While, ast.For, ast.Try)):
                    complexity += 1
                elif isinstance(node, ast.BoolOp):
                    complexity += len(node.values) - 1

            return complexity
        except:
            # Fallback para análise por regex se AST falhar
            patterns = [
                r"\bif\b",
                r"\bwhile\b",
                r"\bfor\b",
                r"\btry\b",
                r"\band\b",
                r"\bor\b",
            ]
            return sum(
                len(re.findall(pattern, code, re.IGNORECASE)) for pattern in patterns
            )

    def _extract_dependencies(self, code: str) -> List[str]:
        """Extrai dependências/imports do código"""
        dependencies = []

        # Python imports
        import_patterns = [
            r"import\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            r"from\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+import",
        ]

        for pattern in import_patterns:
            matches = re.findall(pattern, code)
            dependencies.extend(matches)

        return list(set(dependencies))  # Remove duplicates

    def _count_sql_queries(self, code: str) -> int:
        """Conta queries SQL no código"""
        sql_patterns = [
            r'""".*?SELECT.*?"""',
            r"'''.*?SELECT.*?'''",
            r'".*?SELECT.*?"',
            r"'.*?SELECT.*?'",
        ]

        count = 0
        for pattern in sql_patterns:
            count += len(re.findall(pattern, code, re.DOTALL | re.IGNORECASE))

        return count

    def _extract_spark_operations(self, code: str) -> List[str]:
        """Extrai operações Spark do código"""
        spark_ops = []

        # Padrões comuns do Spark
        patterns = [
            r"\.read\(\)",
            r"\.write\(\)",
            r"\.sql\(",
            r"\.join\(",
            r"\.groupBy\(",
            r"\.agg\(",
            r"\.collect\(\)",
            r"\.show\(\)",
            r"\.cache\(\)",
            r"\.persist\(\)",
        ]

        for pattern in patterns:
            if re.search(pattern, code):
                op_name = pattern.replace("\\", "").replace("(", "").replace(")", "")
                spark_ops.append(op_name)

        return spark_ops

    def _detect_performance_issues(self, code: str) -> List[str]:
        """Detecta possíveis problemas de performance"""
        issues = []

        # Padrões problemáticos
        if re.search(r"\.collect\(\)", code):
            issues.append("collect() operation found - may cause memory issues")

        if re.search(r"for.*\.count\(\)", code, re.DOTALL):
            issues.append("count() in loop detected - inefficient")

        if code.count(".join(") > 5:
            issues.append("Multiple joins detected - consider optimization")

        return issues

    def _detect_security_issues(self, code: str) -> List[str]:
        """Detecta possíveis problemas de segurança"""
        issues = []

        # Hardcoded credentials
        if re.search(
            r'(password|token|key)\s*=\s*["\'][^"\']+["\']', code, re.IGNORECASE
        ):
            issues.append("Potential hardcoded credentials found")

        # SQL injection risks
        if re.search(r'f["\'].*?SELECT.*?\{.*?\}.*?["\']', code, re.IGNORECASE):
            issues.append("Potential SQL injection risk with f-strings")

        return issues

    def _check_best_practices(self, code: str) -> List[str]:
        """Verifica violações de melhores práticas"""
        violations = []

        # Imports não utilizados (simplificado)
        if "import *" in code:
            violations.append("Wildcard imports found (import *)")

        # Linhas muito longas (>120 chars)
        long_lines = [line for line in code.split("\n") if len(line) > 120]
        if long_lines:
            violations.append(f"{len(long_lines)} lines exceed 120 characters")

        return violations
