"""
Setup script for GDPR Compliance Framework
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="databricks-gdpr-compliance",
    version="1.0.0",
    description="GDPR-compliant data governance framework for Databricks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="GDPR Compliance Team",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "databricks-sdk>=0.20.0",
        "databricks-sql-connector>=3.0.0",
        "pyspark>=3.5.0",
        "cryptography>=42.0.0",
        "pandas>=2.0.0",
        "python-dateutil>=2.8.0",
        "sqlalchemy>=2.0.0",
        "pyyaml>=6.0",
        "pydantic>=2.0.0",
        "faker>=20.0.0",
        "tqdm>=4.66.0",
    ],
    entry_points={
        "console_scripts": [
            "gdpr-deploy=deploy:main",
        ],
    },
)

