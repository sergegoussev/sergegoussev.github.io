#!/bin/bash

# 1. Define your repository name (CRITICAL for mimicking GitHub Pages)
#    If your site is at sergegoussev.github.io/my-project/, set this to "my-project"
REPO_NAME="personal-site"

echo "🧹 Cleaning up old builds..."
rm -rf _site/en _site/fr

echo "Rendering English Profile..."
quarto render --profile en

echo "Rendering French Profile..."
quarto render --profile fr

# echo "📂 Organizing folders..."
# # We create a folder structure that mimics the real GitHub Pages URL path
# # localhost:8000/my-project/en/ instead of just localhost:8000/en/
# mkdir -p _gh_preview/$REPO_NAME/en
# mkdir -p _gh_preview/$REPO_NAME/fr

# # Move the rendered sites into the repo-named subfolder
# cp -r _site-en/* _gh_preview/$REPO_NAME/en/
# cp -r _site-fr/* _gh_preview/$REPO_NAME/fr/

# Create the redirect at the repo root
# echo "<meta http-equiv=\"refresh\" content=\"0; url=/$REPO_NAME/en/\">" > _gh_preview/$REPO_NAME/index.html

# Create a root redirect (in case you hit localhost:8000 directly)
# echo "<meta http-equiv=\"refresh\" content=\"0; url=/$REPO_NAME/en/\">" > _gh_preview/index.html

echo "✅ Build Complete!"
echo "🚀 Serving site at http://localhost:8000/"
echo "   (Press Ctrl+C to stop)"

# Serve from the PARENT of the repo folder to simulate the path
python -m http.server 8000 --directory _site