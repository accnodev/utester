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
from argparse import RawTextHelpFormatter

def start_nifi():
    p=subprocess.Popen("mkdir -p /var/run/nifi", shell=True)
    p.wait()
    p=subprocess.Popen("chmod -R 777 /var/run/nifi", shell=True)

def main(args, loglevel):
    start_nifi()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)

    parser.add_argument('-op', '--operation', help='Operation to perform on the nifi node', type=str, required=True)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)