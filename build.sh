#!/bin/bash
# Just used during CI to build static assets and add to release
npm ci
npm run build
pip install build==1.2.1
python -m build
# To release to pypi run below after a build. Make sure to bump version with nbdev_bump_version.
# twine upload --skip-existing dist/*
