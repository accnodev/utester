#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
Unit test Nifi start.
 - Test creates nifi run directory. This will autocreate a directory named '/var/run/nifi'
 - Test gives permissions to the correponding folder '/var/run/nifi'
 - Test restart nifi service
 - Test run Wiremock .jar

Example:

    To test nifi is actively running, on nifi node try:
        python utNifi.py -op status

    To test start nifi service, on nifi node try:
        python utNifi.py -op start

    To test stop nifi service, on nifi node try:
        python utNifi.py -op stop

"""

import argparse
import logging
import subprocess
import time
from argparse import RawTextHelpFormatter

def check_nifi_status():

    # Check nifi status
    p = subprocess.Popen(['systemctl','status','nifi'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    print(out)

def start_nifi():

    # Creates executable folder
    p=subprocess.Popen(['mkdir','-p','/var/run/nifi'], stdout=subprocess.PIPE)
    p.wait()
    p=subprocess.Popen(['chmod','-R', '777','/var/run/nifi'], stdout=subprocess.PIPE)

    # Restart nifi service
    p1 = subprocess.Popen(['systemctl','stop','nifi'], stdout=subprocess.PIPE)
    p1.wait()
    p1 = subprocess.Popen(['systemctl','start','nifi'], stdout=subprocess.PIPE)

def start_wiremock():

    # Executes Wiremock microservice
    p = subprocess.Popen(['nohup','java','-jar','/opt/nifi/wiremock/wiremock-standalone-2.26.3.jar' ,'--port 9091&'], stdout=subprocess.PIPE)

def stop_nifi():

    # Stop nifi service
    p = subprocess.Popen(['systemctl','stop','nifi'], stdout=subprocess.PIPE)

def main(args, loglevel):

    if args.operation == 'status':
        check_nifi_status()

    elif args.operation == 'start':
        start_nifi()
        time.sleep(10)
        start_wiremock()

    elif args.operation == 'stop':
        stop_nifi()

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