#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
Unit test Nifi start.
 - Test creates nifi run directory. This will autocreate a directory named '/var/run/nifi'
 - Test gives permissions to the correponding folder '/var/run/nifi'
 - Test restart nifi service
 - Test run Wiremock .jar

To test nifi is actively running, on nifi node try:
    python utNifi.py -op status

Example:
    python utNifi.py -op start

"""

import argparse
import logging
import subprocess
import time