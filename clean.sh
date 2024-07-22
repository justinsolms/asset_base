#!/bin/bash

# Remove build directories
rm -rf build/
rm -rf dist/
rm -rf *.egg-info
rm -rf .eggs/

# Remove Python cache files
find . -name '*.pyc' -delete
find . -name '__pycache__' -delete

echo "Cleaned previous build artifacts."
