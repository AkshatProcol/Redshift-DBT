#!/bin/bash

# DBT Setup Verification Script
echo "🔍 Verifying DBT Time Setup..."

# Check if dbt is installed
if command -v dbt &> /dev/null; then
    echo "✅ DBT is installed: $(dbt --version | head -n1)"
else
    echo "❌ DBT is not installed"
    echo "   Install with: pip install dbt-core dbt-redshift"
    exit 1
fi

# Check if we're in the right directory
if [ -f "dbt_project.yml" ] && [ -f "profiles.yml" ]; then
    echo "✅ Project files found"
else
    echo "❌ Missing project files"
    exit 1
fi

# Check profiles
echo "📋 Checking profiles configuration..."
if dbt debug --config-dir . | grep -q "Connection test: OK"; then
    echo "✅ Database connection successful"
else
    echo "❌ Database connection failed"
    echo "   Check your Redshift credentials in profiles.yml"
    exit 1
fi

# Check dependencies
echo "📦 Installing dependencies..."
dbt deps

# List models
echo "📊 Available models:"
dbt ls

echo ""
echo "🎉 Setup verification complete!"
echo "🚀 You can now run: ./run_pipeline.sh"
