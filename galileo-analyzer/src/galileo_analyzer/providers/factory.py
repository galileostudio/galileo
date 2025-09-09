from typing import Union
from .aws.glue import GlueProvider
from .databricks.connector import DatabricksProvider
from .snowflake.connector import SnowflakeProvider
from ..core.config import AWSCredentials, DatabricksCredentials, SnowflakeCredentials
from ..core.exceptions import ConfigurationError


class ProviderFactory:
    """Factory for creating cloud providers"""

    @staticmethod
    def create_aws_provider(credentials: AWSCredentials) -> GlueProvider:
        """Create AWS Glue provider"""
        return GlueProvider(credentials)

    @staticmethod
    def create_databricks_provider(
        credentials: DatabricksCredentials,
    ) -> DatabricksProvider:
        """Create Databricks provider (placeholder)"""
        return DatabricksProvider(credentials)

    @staticmethod
    def create_snowflake_provider(
        credentials: SnowflakeCredentials,
    ) -> SnowflakeProvider:
        """Create Snowflake provider (placeholder)"""
        return SnowflakeProvider(credentials)

    @staticmethod
    def create_provider(
        provider_type: str,
        credentials: Union[AWSCredentials, DatabricksCredentials, SnowflakeCredentials],
    ):
        """Create provider based on type"""
        if provider_type.lower() == "aws":
            if not isinstance(credentials, AWSCredentials):
                raise ConfigurationError("AWS provider requires AWSCredentials")
            return ProviderFactory.create_aws_provider(credentials)
        elif provider_type.lower() == "databricks":
            if not isinstance(credentials, DatabricksCredentials):
                raise ConfigurationError(
                    "Databricks provider requires DatabricksCredentials"
                )
            return ProviderFactory.create_databricks_provider(credentials)
        elif provider_type.lower() == "snowflake":
            if not isinstance(credentials, SnowflakeCredentials):
                raise ConfigurationError(
                    "Snowflake provider requires SnowflakeCredentials"
                )
            return ProviderFactory.create_snowflake_provider(credentials)
        else:
            raise ConfigurationError(f"Unknown provider type: {provider_type}")
