#!/bin/bash
pip install nbdev
nbdev_install
npm install
npm run build
nbdev_clean
nbdev_export
nbdev_readme
nbdev_install_hooks

