###############################################################################
# (c) Copyright 2025 DIRAC Consortium for the benefit of the                  #
# GreenDIGIT Project                                                          #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING".   #
#                                                                             #
###############################################################################

import os

from importlib.metadata import version as get_version, PackageNotFoundError

rootPath = os.path.dirname(os.path.realpath(__path__[0]))

try:
    __version__ = get_version(__name__)
    version = __version__
except PackageNotFoundError:
    version = "Unknown"


def extension_metadata():
    return {
        "primary_extension": True,
        "priority": 100,
        "setups": {
            "Dirac-Production": "dips://ccdirac01.in2p3.fr:9135/Configuration/Server",
            "DIRAC-Test": "dips://cctbdirac01.in2p3.fr:9135/Configuration/Server",
        },
        "default_setup": "Dirac-Production",
    }
