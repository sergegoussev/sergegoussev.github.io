#!/bin/bash

echo "🧹 Cleaning up old builds..."
rm -rf _site

echo "Rendering root redirect index.html and move it to _site/ ..."
quarto render index.qmd --to html
cp index.html _site/index.html

echo "Rendering English..."
quarto render --profile en

echo "Rendering French..."
quarto render --profile fr

echo "✅ Build Complete!"
echo "🚀 Serving site at http://localhost:8080/"
echo "   (Press Ctrl+C to stop)"

# Serve from the PARENT of the repo folder to simulate the path
python -m http.server 8000 --directory _site/
# python -m http.server 8080 --directory _site/