"""Application configuration management."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.exceptions import ConfigurationError


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""

    connection_timeout: int = Field(default=30, ge=1, le=300)
    command_timeout: int = Field(default=300, ge=1, le=3600)
    pool_size: int = Field(default=5, ge=1, le=20)
    max_overflow: int = Field(default=10, ge=0, le=50)
    pool_recycle: int = Field(default=3600, ge=300)
    echo: bool = Field(default=False)


class ComparisonModeConfig(BaseSettings):
    """Configuration for a single comparison mode."""

    description: str = Field(default="")
    use_checksums: bool = Field(default=False)
    compare_data: bool = Field(default=True)
    compare_indexes: bool = Field(default=False)
    compare_constraints: bool = Field(default=False)


class ComparisonConfig(BaseSettings):
    """Comparison configuration settings."""

    modes: dict[str, ComparisonModeConfig] = Field(default_factory=dict)
    chunk_size: int = Field(default=10000, ge=100, le=1000000)
    max_parallel_tables: int = Field(default=4, ge=1, le=16)
    ignore_case: bool = Field(default=False)
    ignore_whitespace: bool = Field(default=False)
    date_format: str = Field(default="%Y-%m-%d %H:%M:%S")


class CompressionRecommendationsConfig(BaseSettings):
    """Configuration for compression recommendations."""

    page_min_size_mb: int = Field(default=10, ge=1)
    row_min_size_mb: int = Field(default=5, ge=1)
    columnstore_min_rows: int = Field(default=100000, ge=1000)


class CompressionConfig(BaseSettings):
    """Compression analysis configuration settings."""

    model_config = SettingsConfigDict(extra="ignore")


    analyze_threshold: int = Field(default=1000, ge=0)
    estimate_sample_percent: int = Field(default=10, ge=1, le=100)
    supported_types: list[str] = Field(default=["PAGE", "ROW", "COLUMNSTORE"])
    recommendations: CompressionRecommendationsConfig = Field(
        default_factory=CompressionRecommendationsConfig
    )


class LoggingConfig(BaseSettings):
    """Logging configuration settings."""

    level: str = Field(default="INFO")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    date_format: str = Field(default="%Y-%m-%d %H:%M:%S")
    file_enabled: bool = Field(default=True)
    file_path: str = Field(default="logs/app.log")
    file_max_bytes: int = Field(default=10485760)  # 10MB
    file_backup_count: int = Field(default=5)
    console_enabled: bool = Field(default=True)

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v_upper


class UIConfig(BaseSettings):
    """UI configuration settings."""

    model_config = SettingsConfigDict(extra="ignore")


    primary_color: str = Field(default="#0066CC")
    background_color: str = Field(default="#FFFFFF")
    secondary_background_color: str = Field(default="#F0F2F6")
    text_color: str = Field(default="#262730")
    font: str = Field(default="sans serif")
    match_color: str = Field(default="#28A745")
    schema_diff_color: str = Field(default="#FFC107")
    data_diff_color: str = Field(default="#DC3545")
    missing_color: str = Field(default="#6C757D")
    layout: str = Field(default="wide")
    max_rows_preview: int = Field(default=1000, ge=10, le=100000)


class SecurityConfig(BaseSettings):
    """Security configuration settings."""

    encrypt_credentials: bool = Field(default=True)
    use_parameterized_queries: bool = Field(default=True)
    session_timeout: int = Field(default=3600, ge=300, le=86400)
    max_login_attempts: int = Field(default=5, ge=1, le=10)
    encryption_key: Optional[str] = Field(default=None)


class PerformanceConfig(BaseSettings):
    """Performance configuration settings."""

    cache_enabled: bool = Field(default=True)
    cache_ttl: int = Field(default=3600, ge=60, le=86400)
    lazy_loading: bool = Field(default=True)
    use_generators: bool = Field(default=True)
    connection_pooling: bool = Field(default=True)


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = Field(
        default="SQL Server Data Comparison & Compression Tool",
        alias="APP_NAME",
    )
    app_version: str = Field(default="1.0.0", alias="APP_VERSION")
    app_env: str = Field(default="development", alias="APP_ENV")

    # Database Connection - Source
    source_server: str = Field(default="localhost", alias="SOURCE_SERVER")
    source_database: str = Field(default="", alias="SOURCE_DATABASE")
    source_username: Optional[str] = Field(default=None, alias="SOURCE_USERNAME")
    source_password: Optional[str] = Field(default=None, alias="SOURCE_PASSWORD")
    source_use_windows_auth: bool = Field(
        default=False, alias="SOURCE_USE_WINDOWS_AUTH"
    )

    # Database Connection - Target
    target_server: str = Field(default="localhost", alias="TARGET_SERVER")
    target_database: str = Field(default="", alias="TARGET_DATABASE")
    target_username: Optional[str] = Field(default=None, alias="TARGET_USERNAME")
    target_password: Optional[str] = Field(default=None, alias="TARGET_PASSWORD")
    target_use_windows_auth: bool = Field(
        default=False, alias="TARGET_USE_WINDOWS_AUTH"
    )

    # Performance
    max_workers: int = Field(default=4, ge=1, le=16, alias="MAX_WORKERS")
    chunk_size: int = Field(default=10000, ge=100, le=1000000, alias="CHUNK_SIZE")
    cache_ttl: int = Field(default=3600, ge=60, le=86400, alias="CACHE_TTL")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Sub-configurations
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    comparison: ComparisonConfig = Field(default_factory=ComparisonConfig)
    compression: CompressionConfig = Field(default_factory=CompressionConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)

    def __init__(self, **kwargs: Any) -> None:
        """Initialize settings and load from YAML if available."""
        super().__init__(**kwargs)
        self._load_yaml_config()

    def _load_yaml_config(self) -> None:
        """Load configuration from YAML file."""
        config_path = Path("config/config.yaml")
        if not config_path.exists():
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_config = yaml.safe_load(f)

            if not yaml_config:
                return

            # Load sub-configurations
            if "database" in yaml_config:
                self.database = DatabaseConfig(**yaml_config["database"])

            if "comparison" in yaml_config:
                self.comparison = ComparisonConfig(**yaml_config["comparison"])

            if "compression" in yaml_config:
                self.compression = CompressionConfig(**yaml_config["compression"])

            if "logging" in yaml_config:
                log_config = yaml_config["logging"]
                # Flatten nested file and console configs
                if "file" in log_config:
                    file_config = log_config.pop("file")
                    log_config["file_enabled"] = file_config.get("enabled", True)
                    log_config["file_path"] = file_config.get("path", "logs/app.log")
                    log_config["file_max_bytes"] = file_config.get(
                        "max_bytes", 10485760
                    )
                    log_config["file_backup_count"] = file_config.get(
                        "backup_count", 5
                    )
                if "console" in log_config:
                    console_config = log_config.pop("console")
                    log_config["console_enabled"] = console_config.get("enabled", True)
                self.logging = LoggingConfig(**log_config)

            if "ui" in yaml_config:
                ui_config = yaml_config["ui"]
                # Flatten nested theme and colors configs
                if "theme" in ui_config:
                    theme = ui_config.pop("theme")
                    ui_config.update(theme)
                if "colors" in ui_config:
                    colors = ui_config.pop("colors")
                    ui_config["match_color"] = colors.get("match", "#28A745")
                    ui_config["schema_diff_color"] = colors.get(
                        "schema_diff", "#FFC107"
                    )
                    ui_config["data_diff_color"] = colors.get("data_diff", "#DC3545")
                    ui_config["missing_color"] = colors.get("missing", "#6C757D")
                if "export" in ui_config:
                    export_config = ui_config.pop("export")
                    ui_config["max_rows_preview"] = export_config.get(
                        "max_rows_preview", 1000
                    )
                if "page_config" in ui_config:
                    page_config = ui_config.pop("page_config")
                    ui_config["layout"] = page_config.get("layout", "wide")
                self.ui = UIConfig(**ui_config)

            if "security" in yaml_config:
                self.security = SecurityConfig(**yaml_config["security"])

            if "performance" in yaml_config:
                self.performance = PerformanceConfig(**yaml_config["performance"])

        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration from {config_path}: {str(e)}",
                config_key="yaml_config",
            ) from e

    def get_source_connection_string(self) -> str:
        """
        Get SQL Server connection string for source database.

        Returns:
            Connection string for source database

        Raises:
            ConfigurationError: If required connection parameters are missing
        """
        if not self.source_database:
            raise ConfigurationError(
                "Source database name is required", config_key="source_database"
            )

        driver = "{ODBC Driver 18 for SQL Server}"

        if self.source_use_windows_auth:
            return (
                f"DRIVER={driver};"
                f"SERVER={self.source_server};"
                f"DATABASE={self.source_database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            if not self.source_username or not self.source_password:
                raise ConfigurationError(
                    "Source username and password are required for SQL authentication",
                    config_key="source_credentials",
                )
            return (
                f"DRIVER={driver};"
                f"SERVER={self.source_server};"
                f"DATABASE={self.source_database};"
                f"UID={self.source_username};"
                f"PWD={self.source_password};"
                f"TrustServerCertificate=yes;"
            )

    def get_target_connection_string(self) -> str:
        """
        Get SQL Server connection string for target database.

        Returns:
            Connection string for target database

        Raises:
            ConfigurationError: If required connection parameters are missing
        """
        if not self.target_database:
            raise ConfigurationError(
                "Target database name is required", config_key="target_database"
            )

        driver = "{ODBC Driver 18 for SQL Server}"

        if self.target_use_windows_auth:
            return (
                f"DRIVER={driver};"
                f"SERVER={self.target_server};"
                f"DATABASE={self.target_database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
        else:
            if not self.target_username or not self.target_password:
                raise ConfigurationError(
                    "Target username and password are required for SQL authentication",
                    config_key="target_credentials",
                )
            return (
                f"DRIVER={driver};"
                f"SERVER={self.target_server};"
                f"DATABASE={self.target_database};"
                f"UID={self.target_username};"
                f"PWD={self.target_password};"
                f"TrustServerCertificate=yes;"
            )


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached application settings.

    Returns:
        Application settings instance
    """
    return Settings()
