#!/usr/bin/env python3
"""
ASX Stock Analysis - Main Launcher.

This script provides a convenient way to run all components of the application.

Usage:
    python main.py dashboard    # Run the main dashboard
    python main.py signals      # Run the signals overview
    python main.py ml-app       # Run the ML training app
    python main.py extract      # Run data extraction
    python main.py extract-inc  # Run incremental extraction
    python main.py dbt          # Run dbt models
    python main.py test         # Run tests
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_streamlit(app: str, port: int = 8501):
    """Run a Streamlit app."""
    app_path = Path(__file__).parent / "app" / f"{app}.py"

    if not app_path.exists():
        logger.error(f"App not found: {app_path}")
        return 1

    logger.info(f"Starting Streamlit app: {app} on port {port}")
    cmd = ["uv", "run", "streamlit", "run", str(app_path), "--server.port", str(port)]

    return subprocess.call(cmd)


def run_extraction(incremental: bool = False):
    """Run data extraction."""
    cmd = ["uv", "run", "python", "-m", "ingestion.asx_extraction.extract"]

    if incremental:
        cmd.append("run-incremental")
    else:
        cmd.append("run")

    logger.info(f"Running extraction: {'incremental' if incremental else 'full'}")
    return subprocess.call(cmd)


def run_dbt():
    """Run dbt models."""
    os.chdir(Path(__file__).parent / "dbt_project")

    logger.info("Running dbt deps")
    result = subprocess.call(["uv", "run", "dbt", "deps"])
    if result != 0:
        logger.error("dbt deps failed")
        return result

    logger.info("Running dbt run")
    result = subprocess.call(["uv", "run", "dbt", "run"])
    if result != 0:
        logger.error("dbt run failed")
        return result

    logger.info("Running dbt test")
    return subprocess.call(["uv", "run", "dbt", "test"])


def run_ml_training():
    """Run ML model training."""
    cmd = [
        "uv",
        "run",
        "python",
        "-c",
        """
from ml_models.train import train_model

# Train for all stocks with default settings
result = train_model(symbol=None, model_type='random_forest', threshold=1.0)
print(f"Training complete! Accuracy: {result['metrics']['accuracy']:.2%}")
print(f"Feature importance saved to model")
""",
    ]

    logger.info("Running ML training")
    return subprocess.call(cmd)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="ASX Stock Analysis Launcher")
    parser.add_argument(
        "command",
        choices=[
            "dashboard",
            "signals",
            "ml-app",
            "extract",
            "extract-inc",
            "dbt",
            "ml-train",
            "all",
            "help",
        ],
        default="help",
    )
    parser.add_argument("--port", type=int, default=8501, help="Streamlit port")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    commands = {
        "dashboard": lambda: run_streamlit("dashboard", args.port),
        "signals": lambda: run_streamlit("signals", args.port + 1),
        "ml-app": lambda: run_streamlit("ml_app", args.port + 2),
        "extract": lambda: run_extraction(incremental=False),
        "extract-inc": lambda: run_extraction(incremental=True),
        "dbt": run_dbt,
        "ml-train": run_ml_training,
    }

    if args.command == "help":
        print(__doc__)
        print("\nExamples:")
        print("  python main.py dashboard    # Start the main dashboard")
        print("  python main.py signals      # Start the signals overview")
        print("  python main.py extract     # Run full data extraction")
        print("  python main.py extract-inc # Run incremental extraction")
        print("  python main.py dbt         # Build dbt models")
        print("  python main.py ml-train    # Train ML model")
        print("  python main.py all         # Run full pipeline (extract + dbt)")
        return 0

    if args.command == "all":
        logger.info("Running full pipeline...")
        result = run_extraction(incremental=False)
        if result != 0:
            logger.error("Extraction failed")
            return result

        result = run_dbt()
        if result != 0:
            logger.error("dbt failed")
            return result

        logger.info("Full pipeline complete!")
        return 0

    # Run the command
    return commands.get(args.command, lambda: 1)()


if __name__ == "__main__":
    sys.exit(main())
