#!/bin/bash
# Just used during CI to build static assets and add to release
npm ci
npm run build
pip install build==1.2.1
python -m build