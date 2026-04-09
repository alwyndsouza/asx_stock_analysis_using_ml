"""
Configuration management module.

Loads configuration from config.yaml and provides easy access to settings.
"""

import os
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Configuration manager for ASX Stock Analysis."""

    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Load configuration from YAML file."""
        config_path = Path(__file__).parent.parent / "config.yaml"

        if not config_path.exists():
            # Use defaults
            self._config = self._get_defaults()
            return

        try:
            with open(config_path) as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            self._config = self._get_defaults()

    def _get_defaults(self) -> dict:
        """Return default configuration."""
        return {
            "database": {
                "path": "asx_stocks.duckdb",
                "schema": "raw_asx_data",
                "analytics_schema": "analytics",
            },
            "extraction": {
                "stocks": {
                    "gold": ["EVN.AX", "NST.AX", "RRL.AX", "SBM.AX"],
                    "oil": ["WDS.AX", "STO.AX", "BPT.AX", "KAR.AX"],
                    "silver": ["SVL.AX", "S32.AX"],
                },
                "full_refresh_days": 1825,
                "incremental_days": 30,
                "requests_per_second": 2,
                "retry_attempts": 3,
                "retry_delay_seconds": 5,
            },
            "ml": {
                "models_dir": "ml_models",
                "default_threshold": 1.0,
                "test_size": 0.2,
            },
            "logging": {
                "level": "INFO",
            },
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get config value by dot-notation key."""
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    @property
    def database_path(self) -> str:
        return self.get("database.path", "asx_stocks.duckdb")

    @property
    def database_schema(self) -> str:
        return self.get("database.schema", "raw_asx_data")

    @property
    def stocks(self) -> dict:
        return self.get("extraction.stocks", {})

    @property
    def all_stock_symbols(self) -> list[str]:
        """Get all stock symbols as a flat list."""
        stocks = self.stocks
        return [sym for symbols in stocks.values() for sym in symbols]

    @property
    def incremental_days(self) -> int:
        return self.get("extraction.incremental_days", 30)

    @property
    def full_refresh_days(self) -> int:
        return self.get("extraction.full_refresh_days", 1825)

    @property
    def ml_models_dir(self) -> str:
        return self.get("ml.models_dir", "ml_models")

    @property
    def log_level(self) -> str:
        return self.get("logging.level", "INFO")


# Global config instance
config = Config()
