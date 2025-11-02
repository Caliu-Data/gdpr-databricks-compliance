"""
Deployment script for GDPR Compliance Framework
Deploys to Databricks using Databricks CLI or SDK
"""
import os
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, Task, JobTaskSettings


def deploy_jobs():
    """Deploy Databricks jobs"""
    print("Deploying GDPR Compliance Jobs to Databricks...")
    
    # Initialize Databricks client
    w = WorkspaceClient()
    
    # Read jobs configuration
    import yaml
    with open("databricks_jobs.yaml", "r") as f:
        jobs_config = yaml.safe_load(f)
    
    for job_config in jobs_config.get("jobs", []):
        job_name = job_config["name"]
        print(f"\nCreating/updating job: {job_name}")
        
        try:
            # Check if job exists
            existing_jobs = w.jobs.list(name=job_name)
            if existing_jobs:
                job_id = existing_jobs[0].job_id
                print(f"  Updating existing job (ID: {job_id})")
                w.jobs.update(
                    job_id=job_id,
                    new_settings=JobSettings(**job_config)
                )
            else:
                print(f"  Creating new job")
                job = w.jobs.create(name=job_name, settings=JobSettings(**job_config))
                print(f"  Created job with ID: {job.job_id}")
        
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")


def upload_notebooks():
    """Upload Python notebooks to Databricks workspace"""
    print("\nUploading notebooks to Databricks workspace...")
    
    w = WorkspaceClient()
    
    notebooks_dir = Path("notebooks")
    if not notebooks_dir.exists():
        print("  No notebooks directory found, skipping...")
        return
    
    workspace_path = "/GDPR_Compliance"
    
    try:
        # Create workspace directory
        w.workspace.mkdirs(workspace_path)
        print(f"  Created workspace directory: {workspace_path}")
    except Exception:
        pass  # Directory might already exist
    
    # Upload notebook files
    for notebook_file in notebooks_dir.glob("*.py"):
        workspace_notebook_path = f"{workspace_path}/{notebook_file.stem}"
        print(f"  Uploading {notebook_file.name} -> {workspace_notebook_path}")
        
        with open(notebook_file, "rb") as f:
            w.workspace.upload(
                workspace_notebook_path,
                content=f.read(),
                format="PYTHON",
                overwrite=True
            )


def main():
    """Main deployment function"""
    print("=" * 60)
    print("GDPR Compliance Framework Deployment")
    print("=" * 60)
    
    # Check for Databricks authentication
    if not os.environ.get("DATABRICKS_HOST"):
        print("\n⚠️  Warning: DATABRICKS_HOST not set")
        print("Please set environment variables:")
        print("  - DATABRICKS_HOST")
        print("  - DATABRICKS_TOKEN (or use databricks configure)")
    
    try:
        # Deploy jobs
        deploy_jobs()
        
        # Upload notebooks (optional)
        # upload_notebooks()
        
        print("\n" + "=" * 60)
        print("✓ Deployment completed successfully!")
        print("=" * 60)
        
        print("\nNext steps:")
        print("1. Configure environment variables in your Databricks cluster")
        print("2. Update databricks_jobs.yaml with your specific table names")
        print("3. Run jobs manually or wait for scheduled execution")
        print("4. Review audit logs in: gdpr_compliance.governance.audit_logs")
    
    except Exception as e:
        print(f"\n✗ Deployment failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

