#!/usr/bin/env python
# This is meant to be run to setup and prep for committing a release
# use nbdev_bump_version to increment the version itself then rerun this to update README.md _docs etc
from subprocess import run
import configparser, re

run(["pip", "install", "nbdev"])
run(["pip", "install", "-e", "."]) # get current project in dev mode
run("quarto --version || nbdev_install", shell=True)
run(["npx", "npm-check-updates", "-u"]) # convenient way to freshen package.json on each release
run(["npm", "install"])
run(["npm", "run", "build"])
run(["nbdev_clean"])
run(["nbdev_export"])
run(["nbdev_readme"])
run(["nbdev_docs"])