#!/bin/bash

# As the multilingual site is composed of multiple Quarto projects, 
# we need to render each project individually before serving the site. 
# This script automates that process.

# 1. Render Root first. 
# This ensures _site is created (and cleaned) before we add sub-projects.
echo "Rendering Root..."
quarto render .

# 2. Render Language Projects
echo "Rendering English..."
quarto render en

echo "Rendering Russian..."
quarto render ru

echo "Rendering French..."
quarto render fr

# 3. Serve the site
echo "🚀 Serving site at http://localhost:8000/"
echo "Note - you must go to a browser, paste the URL to view the site."
python -m http.server 8000 --directory _site
