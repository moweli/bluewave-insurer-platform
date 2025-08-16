#!/bin/bash

# Upload notebooks to Databricks workspace
# Requires Databricks CLI to be configured

WORKSPACE_URL="https://adb-1947030536765927.7.azuredatabricks.net"
NOTEBOOKS_DIR="notebooks"

echo "📚 Uploading notebooks to Databricks..."
echo "Workspace: $WORKSPACE_URL"
echo ""

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not installed!"
    echo ""
    echo "To install and configure:"
    echo "1. Install: pip install databricks-cli"
    echo "2. Configure: databricks configure --token"
    echo "   Host: $WORKSPACE_URL"
    echo "   Token: (generate from Databricks UI > Settings > User Settings > Access Tokens)"
    echo ""
    echo "Alternative: Manual upload"
    echo "1. Open $WORKSPACE_URL"
    echo "2. Navigate to Workspace > Users > your-email"
    echo "3. Import notebooks from:"
    echo "   - $PWD/notebooks/00_setup/*.py"
    echo ""
    exit 1
fi

# Create workspace directories
echo "Creating workspace directories..."
databricks workspace mkdirs /demo/00_setup 2>/dev/null
databricks workspace mkdirs /demo/01_bronze 2>/dev/null
databricks workspace mkdirs /demo/02_silver 2>/dev/null
databricks workspace mkdirs /demo/03_gold 2>/dev/null
databricks workspace mkdirs /demo/04_gdpr 2>/dev/null

# Upload setup notebooks
echo "Uploading setup notebooks..."
for notebook in notebooks/00_setup/*.py; do
    if [ -f "$notebook" ]; then
        filename=$(basename "$notebook")
        echo "  Uploading $filename..."
        databricks workspace import "$notebook" "/demo/00_setup/$filename" --language PYTHON --overwrite
    fi
done

echo ""
echo "✅ Notebooks uploaded successfully!"
echo ""
echo "To run the notebooks:"
echo "1. Open $WORKSPACE_URL"
echo "2. Navigate to Workspace > demo"
echo "3. Run notebooks in order:"
echo "   - 00_setup/01_create_databases.py"
echo "   - 00_setup/02_generate_sample_data.py"
echo ""
echo "Remember to:"
echo "1. Create a cluster first (single-node for cost savings)"
echo "2. Attach notebook to cluster before running"
echo "3. Stop cluster after demo to save costs!"