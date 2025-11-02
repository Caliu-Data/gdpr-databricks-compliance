# PowerShell setup script using uv package manager

Write-Host "🚀 Setting up GDPR Compliance Framework with uv..." -ForegroundColor Cyan

# Check if uv is installed
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "❌ uv is not installed. Installing uv..." -ForegroundColor Yellow
    # Install uv (Windows)
    Invoke-WebRequest -Uri "https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-pc-windows-msvc.zip" -OutFile "uv.zip"
    Expand-Archive -Path "uv.zip" -DestinationPath ".\uv" -Force
    $env:Path += ";$(Resolve-Path .\uv).Path"
    Remove-Item "uv.zip"
}

Write-Host "✅ uv is installed" -ForegroundColor Green

# Create virtual environment
Write-Host "📦 Creating virtual environment..." -ForegroundColor Cyan
uv venv

# Activate virtual environment
& .\.venv\Scripts\Activate.ps1

# Install dependencies
Write-Host "📥 Installing dependencies..." -ForegroundColor Cyan
uv pip install -r requirements.txt

# Install package in development mode
Write-Host "🔧 Installing package in development mode..." -ForegroundColor Cyan
uv pip install -e .

Write-Host ""
Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Copy env.example to .env: Copy-Item env.example .env"
Write-Host "2. Edit .env with your Databricks credentials"
Write-Host "3. Generate encryption keys: openssl rand -hex 32"
Write-Host "4. Run: python deploy.py"
Write-Host ""

