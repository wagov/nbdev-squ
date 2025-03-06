# Install project to edit locally
install:
    pip install nbdev ipython==8.32.0
    pip install -e .  # get current project in dev mode
    quarto --version || nbdev_install
    npx -y npm-check-updates -u  # convenient way to freshen package.json on each release
    npm install
    npm run build
    nbdev_clean
    nbdev_export
    nbdev_readme
    nbdev_docs

# Bump version ready for a release
bump_version: install
    nbdev_bump_version

# Build wheels for pypi
build: install
    npm ci
    npm run build
    pip install build==1.2.2
    python -m build

# Upload releases to pypi
release-pypi:
    pip install twine
    twine upload --skip-existing dist/*