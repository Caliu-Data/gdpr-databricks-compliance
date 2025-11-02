# GDPR Compliance Framework for Databricks

A comprehensive data governance framework ensuring GDPR compliance in Databricks. This framework provides automated lineage tracking, retention policies, audit trails, PII detection, data quality validation, pseudonymization, k-anonymity protection, and privacy-by-design principles.

## 🎯 Features

### Core Compliance Features

✅ **Automatic Lineage Tracking** - Know where your data came from and where it goes  
✅ **7-Year Retention Policies** - Automatically enforced data retention  
✅ **Complete Audit Trail** - Prove compliance with detailed logging  
✅ **Automatic PII Detection** - Detects emails, phones, SSNs, and more  
✅ **Data Quality Validation** - Reject bad data early  
✅ **Reversible Pseudonymization** - Process data safely with encryption  
✅ **K-Anonymity for Analytics** - Prevent re-identification  
✅ **Aggregations Only** - No raw PII in analytics  
✅ **Privacy by Design** - Compliant from the start  

## 📋 GDPR Articles Covered

| GDPR Article | Requirement | Implementation |
|-------------|-------------|----------------|
| **Art. 4(5)** | Pseudonymization | ✅ Reversible pseudonymization module |
| **Art. 5(1)(e)** | Storage limitation | ✅ 7-year retention policies with automatic enforcement |
| **Art. 17** | Right to erasure | ✅ Retention policies with soft/hard delete |
| **Art. 25** | Data protection by design | ✅ Privacy-by-design framework integration |
| **Art. 30** | Records of processing | ✅ Complete audit trail and lineage tracking |
| **Art. 32** | Security of processing | ✅ Encryption, pseudonymization, access logging |

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Databricks workspace access
- Databricks token or authentication configured
- [uv](https://github.com/astral-sh/uv) package manager (recommended) or pip

### Installation

#### Using uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv pip install -r requirements.txt

# Or install in development mode
uv pip install -e .
```

#### Using pip

```bash
pip install -r requirements.txt
# Or
pip install -e .
```

### Configuration

1. Copy the example environment file:
```bash
cp env.example .env
```

2. Edit `.env` with your Databricks credentials:
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-token
DATABRICKS_CATALOG=gdpr_compliance
DATABRICKS_SCHEMA=governance
```

3. Generate encryption keys:
```bash
# Generate pseudonymization key
openssl rand -hex 32

# Generate encryption key
openssl rand -hex 32
```

Add these keys to your `.env` file.

### Initial Setup in Databricks

1. Create the governance catalog and schema:
```sql
CREATE CATALOG IF NOT EXISTS gdpr_compliance;
CREATE SCHEMA IF NOT EXISTS gdpr_compliance.governance;
```

2. The framework will automatically create the following tables:
   - `audit_logs` - Complete audit trail
   - `data_lineage` - Data lineage tracking
   - `pii_registry` - PII detection registry

## 📚 Usage

### Basic Usage

```python
from src.config import load_config
from src.governance import GDPRGovernanceFramework
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("GDPR_Compliance").getOrCreate()

# Load configuration
config = load_config()

# Initialize framework
framework = GDPRGovernanceFramework(config=config, spark_session=spark)
```

### Scan for PII

```python
# Scan a table for PII
result = framework.scan_for_pii(
    table_name="catalog.schema.customer_data",
    user="data_engineer",
    sample_size=1000
)

print(result["summary"])
```

### Process Data with Compliance

```python
# Process data with automatic PII detection and pseudonymization
result = framework.process_with_compliance(
    source_table="catalog.schema.raw_customers",
    target_table="catalog.schema.processed_customers",
    user="data_engineer",
    pii_columns=["email", "phone", "ssn"],  # Auto-detect if None
    validate_quality=True,
    ensure_k_anonymity=True,
    quasi_identifiers=["age", "zip_code", "gender"]
)
```

### Register Retention Policies

```python
# Register a retention policy
framework.retention_manager.register_policy(
    table_name="catalog.schema.transactions",
    date_column="transaction_date",
    retention_years=7,
    enabled=True,
    soft_delete=False  # Hard delete after retention
)

# Apply retention policies
results = framework.apply_retention_policies()
```

### Create Aggregate-Only Views

```python
# Create view with only aggregations (no raw PII)
framework.create_aggregate_view(
    source_table="catalog.schema.customer_data",
    target_view="catalog.schema.customer_analytics",
    group_by_columns=["region", "age_group"],
    aggregate_columns={
        "revenue": "sum",
        "transaction_count": "count",
        "avg_order_value": "mean"
    }
)
```

### Generate Compliance Report

```python
# Generate comprehensive compliance report
report = framework.get_compliance_report(
    table_name="catalog.schema.customer_data",
    user="data_engineer"
)

print(f"PII Detected: {report['pii_status']['pii_detected']}")
print(f"Quality Score: {report.get('quality_score', 'N/A')}")
```

## 🔧 Databricks Jobs

The framework includes pre-configured Databricks jobs for automated compliance:

### 1. Daily GDPR Compliance Checks
Runs daily at 2 AM UTC to:
- Apply retention policies
- Scan registered tables for PII
- Validate data quality

### 2. PII Detection Job
Scans specified tables for PII detection:
```bash
python jobs/pii_detection_job.py \
    --tables catalog.schema.table1 catalog.schema.table2 \
    --sample-size 1000
```

### 3. Data Processing Job
Processes data with full GDPR compliance:
```bash
python jobs/data_processing_job.py \
    --source-table catalog.schema.raw_data \
    --target-table catalog.schema.processed_data \
    --ensure-k-anonymity \
    --quasi-identifiers age zip_code gender
```

### 4. Compliance Report Job
Generates weekly compliance reports:
```bash
python jobs/compliance_report_job.py \
    --tables catalog.schema.table1 catalog.schema.table2 \
    --output-path /dbfs/reports/compliance.json
```

## 📦 Deployment

### Deploy Jobs to Databricks

```bash
# Using the deployment script
python deploy.py

# Or manually using Databricks CLI
databricks jobs deploy databricks_jobs.yaml
```

### Configure Jobs

Edit `databricks_jobs.yaml` to customize:
- Table names to scan
- Schedule times
- Cluster configurations
- Notification settings

## 🔍 Modules

### PII Detection (`src/pii_detection.py`)
- Automatic detection of emails, phones, SSNs, credit cards, IPs
- Configurable confidence thresholds
- Sample-based scanning for performance

### Lineage Tracking (`src/lineage.py`)
- Automatic tracking of data flow
- Upstream and downstream lineage queries
- Operation type tracking (read, write, transform)

### Audit Logging (`src/audit.py`)
- Comprehensive audit trail
- Event type classification
- Queryable audit logs with filters

### Retention Policies (`src/retention.py`)
- Automatic 7-year retention enforcement
- Soft delete and hard delete options
- Configurable per-table policies

### Pseudonymization (`src/pseudonymization.py`)
- Reversible encryption-based pseudonymization
- Deterministic mode for consistent mapping
- Column-level and DataFrame-level operations

### K-Anonymity (`src/k_anonymity.py`)
- K-anonymity checking and enforcement
- Generalization strategies
- Row suppression for small groups
- Aggregate-only view creation

### Data Quality (`src/data_quality.py`)
- Configurable validation rules
- Not-null, uniqueness, range, format checks
- Referential integrity validation
- Quality scoring

## 🛡️ Security Best Practices

1. **Key Management**: Store encryption keys in Databricks secrets:
   ```bash
   databricks secrets create-scope gdpr-compliance
   databricks secrets put gdpr-compliance pseudonymization_key
   ```

2. **Access Control**: Use Databricks access control lists (ACLs) for governance tables
3. **Audit Logs**: Regularly review audit logs for anomalies
4. **Retention Policies**: Test retention policies on non-production data first

## 📊 Monitoring

### View Audit Logs
```sql
SELECT * FROM gdpr_compliance.governance.audit_logs
WHERE table_name = 'catalog.schema.your_table'
ORDER BY timestamp DESC
LIMIT 100;
```

### View Lineage
```sql
SELECT * FROM gdpr_compliance.governance.data_lineage
WHERE source_table = 'catalog.schema.your_table'
OR target_table = 'catalog.schema.your_table';
```

### Check Retention Status
```python
summary = framework.retention_manager.get_policy_summary()
print(summary)
```

## 📝 Configuration Reference

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Databricks workspace URL | Yes |
| `DATABRICKS_TOKEN` | Databricks access token | Yes |
| `DATABRICKS_CATALOG` | Catalog for governance tables | No (default: gdpr_compliance) |
| `DATABRICKS_SCHEMA` | Schema for governance tables | No (default: governance) |
| `PSEUDONYMIZATION_KEY` | 32-byte hex key for pseudonymization | Yes |
| `RETENTION_YEARS` | Default retention period | No (default: 7) |
| `K_ANONYMITY_THRESHOLD` | Minimum k value | No (default: 5) |
| `MIN_DATA_QUALITY_SCORE` | Minimum quality score | No (default: 0.8) |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request


## ⚠️ Disclaimer

This framework provides tools and infrastructure to help with GDPR compliance but does not guarantee compliance. Ensure you:
- Review and customize policies for your use case
- Consult with legal experts for GDPR compliance
- Regularly audit and update your data governance practices
- Test thoroughly in non-production environments

## 🔗 Resources

- [GDPR Official Website](https://gdpr.eu/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Data Privacy Best Practices](https://databricks.com/blog/2020/03/27/data-governance-with-delta-lake-building-reliable-and-secure-data-lakes-at-scale.html)

## 📧 Support

For issues and questions:
- Open an issue on GitHub
- Contact your data governance team
- Review the audit logs for operational issues

---

**Built with ❤️ from Berlin for GDPR Compliance**

