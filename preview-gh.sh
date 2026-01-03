#!/bin/bash
## Script to preview the site locally - which is difficult for 
## multilingual sites as there is multiple renders

echo "🧹 Cleaning up old builds..."
rm -rf _site

echo "Rendering English..."
quarto render --profile en

echo "Rendering French..."
quarto render --profile fr

echo "render Root... "
quarto render redirect.qmd -o index.html

echo "✅ Build Complete!"
echo "🚀 Serving site at http://localhost:8000/"
echo "   (Press Ctrl+C to stop)"

# Serve from the PARENT of the repo folder to simulate the path
python -m http.server 8000 --directory _site/
# python -m http.server 8080 --directory _site/