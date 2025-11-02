# Quick Start Guide

## 🚀 5-Minute Setup

### 1. Install Dependencies

**Using uv (Recommended):**
```bash
# Windows PowerShell
.\setup_with_uv.ps1

# Linux/Mac
chmod +x setup_with_uv.sh
./setup_with_uv.sh
```

**Using pip:**
```bash
pip install -r requirements.txt
pip install -e .
```

### 2. Configure Environment

```bash
# Copy example config
cp env.example .env

# Edit .env with your settings
# Generate keys:
openssl rand -hex 32  # For PSEUDONYMIZATION_KEY
openssl rand -hex 32  # For ENCRYPTION_KEY
```

### 3. Initialize Databricks

```bash
# Run initialization script
python utils/init_databricks.py
```

This creates:
- `gdpr_compliance.governance.audit_logs`
- `gdpr_compliance.governance.data_lineage`
- `gdpr_compliance.governance.pii_registry`

### 4. Deploy Jobs (Optional)

```bash
python deploy.py
```

## 📝 Common Operations

### Scan for PII

```python
from src.governance import GDPRGovernanceFramework
from src.config import load_config

framework = GDPRGovernanceFramework(config=load_config())
result = framework.scan_for_pii("catalog.schema.table", user="you")
```

### Process Data with Compliance

```python
framework.process_with_compliance(
    source_table="catalog.schema.raw",
    target_table="catalog.schema.processed",
    user="data_engineer",
    validate_quality=True,
    ensure_k_anonymity=True
)
```

### Register Retention Policy

```python
framework.retention_manager.register_policy(
    table_name="catalog.schema.transactions",
    date_column="transaction_date",
    retention_years=7
)
```

### Generate Compliance Report

```python
report = framework.get_compliance_report("catalog.schema.table")
print(report)
```

## 🔧 Running Jobs

### Daily Compliance Checks
```bash
python jobs/daily_gdpr_checks.py
```

### PII Detection
```bash
python jobs/pii_detection_job.py --tables catalog.schema.table1 --sample-size 1000
```

### Data Processing
```bash
python jobs/data_processing_job.py \
    --source-table catalog.schema.raw \
    --target-table catalog.schema.processed \
    --ensure-k-anonymity
```

## 📊 Monitoring

### View Audit Logs
```sql
SELECT * FROM gdpr_compliance.governance.audit_logs
ORDER BY timestamp DESC
LIMIT 100;
```

### Check Lineage
```sql
SELECT * FROM gdpr_compliance.governance.data_lineage
WHERE source_table = 'your_table';
```

## ⚠️ Important Notes

1. **Keys**: Store encryption keys securely (use Databricks secrets in production)
2. **Testing**: Test all policies on non-production data first
3. **Permissions**: Configure Databricks ACLs for governance tables
4. **Backup**: Backup pseudonymization mapping keys for recovery

## 🆘 Troubleshooting

### "Spark session required" error
- Ensure you're running in a Databricks environment with Spark available
- Or initialize: `SparkSession.builder.appName("GDPR").getOrCreate()`

### "Configuration not found" error
- Check `.env` file exists and contains all required variables
- Verify encryption keys are valid hex strings (64 characters)

### Table access denied
- Check Databricks permissions
- Ensure catalog/schema exist and are accessible

## 📚 Next Steps

- Review `README.md` for detailed documentation
- Check `examples/basic_usage.py` for usage examples
- Customize `databricks_jobs.yaml` for your tables
- Set up scheduled jobs for automated compliance

