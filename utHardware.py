#!/usr/bin/env python
# # -*- coding: utf-8 -*-

# TODO: Modify the help message based on all the hardware checks
"""
Unit test the hardware of a machine.
 - Test Producer from command lines. This will autocreate a topic named 'utester'
 - Test Delete topic
 - Test Describe resource with filter

To test consumer is working, on kafka server try:
    kafka-console-consumer --bootstrap-server localhost:9092 --topic utester --from-beginning

Example:
    Create lines
        python utKafka.py -b 192.168.56.51:9092 -pl

    Describe topic
        python utKafka.py -b 192.168.56.51:9092 -d topic -cf utester

    Delete Topic
        python utKafka.py -b 192.168.56.51:9092 -dt

"""

import argparse
import logging

from pathlib import Path
from argparse import RawTextHelpFormatter

from helpers.utils import *
#from helpers.hardware import *

log = logging.getLogger(Path(__file__).stem)
logfile = 'operations.log'
version = "1.0"


def check_hardware(config):
    log.debug("------------------ Begin check_hardware ------------------")
    log_trace = 'None'
    status = 'Ok'

    # TODO: Remove this if no global object is needed in the following methods.
    # try:
    #     conf = {'bootstrap.servers': config['broker']}
    #     producer = Producer(**conf)
    #
    # except Exception as ex:
    #     e, _, ex_traceback = sys.exc_info()
    #     log_traceback(log, ex, ex_traceback)
    #     return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}

    # ------------------------- Switch options ------------------------- #
    # TODO: I think here we don't need ifs. All the hardware checks need to be done.
    # if config['producelines']:
    #     publish_lines(producer, config['topic'])
    #
    # if config['listtopics']:
    #     a = create_admin_client(config['broker'])
    #     list_topics(a, config['topic'])
    #
    # if config['deletetopic']:
    #     a = create_admin_client(config['broker'])
    #     delete_topic(a, config['topic'])
    #
    # if config['describe'] != 'unknown':
    #     a = create_admin_client(config['broker'])
    #     describe_configs(a, config['describe'], config['configfilter'])
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End check_hardware ------------------")
    return {"logtrace": log_trace, "status": status}


# TODO: Implement here one method for each hardware check


def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started check_hardware')
    log.debug("------------------ Reading config ------------------")

    config = {
        'configfile': args.configfile,
        'type': args.type,
        'fqdn': args.fqdn,
    }
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    hw_info = check_hardware(config)

    print("Done.")
    logging.info('Finished check_hardware')
    exit_to_icinga(hw_info)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)

    # TODO: default="none" or default=None or without default?
    parser.add_argument('-c', '--configfile', help='Configuration file path (.json).', type=str, default=None, required=True)
    parser.add_argument('-t', '--type', help='Machine type (i.e. kafka).', type=str, default=None, required=True)
    parser.add_argument('-f', '--fqdn', help='Fully Qualified Domain Name.', type=str, default=None, required=True)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)
