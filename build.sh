#!/bin/bash
# Just used during CI to build static assets and add to release
npm install
npm run build
pip install build
python -m build