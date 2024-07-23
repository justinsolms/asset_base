#!/bin/bash

# Remove build directories
rm -rf build/
rm -rf dist/
find . -name "*.egg-info" | xargs rm -rf

# Remove Python cache files
find . -name '*.pyc' -delete
find . -name '__pycache__'  | xargs rm -rf

echo "Cleaned previous build artifacts."
