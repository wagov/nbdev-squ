"""
Compatibility module for backwards compatibility.
This allows existing code to continue using 'from nbdev_squ import ...'
"""

import warnings

# Import everything from the new wagov_squ module
from wagov_squ import *  # noqa: F403, F401

# Import submodules to make them available
from wagov_squ import api, core, legacy  # noqa: F401

# Issue a deprecation warning
warnings.warn(
    "The 'nbdev_squ' import name is deprecated. Please use 'wagov_squ' instead. "
    "Support for 'nbdev_squ' will be removed in a future version.",
    DeprecationWarning,
    stacklevel=2
)
