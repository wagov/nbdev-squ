"""
Compatibility module for backwards compatibility.
This allows existing code to continue using 'from nbdev_squ import ...'
"""

import sys
import warnings
import wagov_squ

# Issue a deprecation warning
warnings.warn(
    "The 'nbdev_squ' import name is deprecated. Please use 'wagov_squ' instead. "
    "Support for 'nbdev_squ' will be removed in a future version.",
    DeprecationWarning,
    stacklevel=2
)

# Make nbdev_squ act as an alias to wagov_squ
sys.modules[__name__] = wagov_squ
