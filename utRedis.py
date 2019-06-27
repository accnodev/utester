#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""
Unit test a Redis deployment.
"""

import argparse
import logging
import os
import sys

from pathlib import Path
from argparse import RawTextHelpFormatter

import redis
from helpers.utils import *
# from helpers.redis import *

log = logging.getLogger(Path(__file__).stem)
logfile = 'operations.log'
version = "1.0"


def send_to_redis(config):
    log.debug("------------------ Begin send_to_redis ------------------")
    log_trace = 'None'
    status = 'Ok'

    # try:
    #     conf = {'bootstrap.servers': config['broker']}
    #     producer = Producer(**conf)
    #
    # except Exception as ex:
    #     e, _, ex_traceback = sys.exc_info()
    #     log_traceback(log, ex, ex_traceback)
    #     return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}

    # ------------------------- Switch options ------------------------- #
    hello_redis(config)

    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End send_to_redis ------------------")
    return {"logtrace": log_trace, "status": status}


def hello_redis(config):
    """Hello Redis Program"""

    # step 1: create the Redis Connection object
    try:

        # The decode_repsonses flag here directs the client to convert the responses from Redis into Python strings
        # using the default encoding utf-8.  This is client specific.
        if config['password']:
            r = redis.StrictRedis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
        else:
            r = redis.StrictRedis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)

        # step 2: Set the hello message in Redis
        r.set("msg:hello", "Hello Redis!!!")

        # step 3: Retrieve the hello message from Redis
        msg = r.get("msg:hello")
        print(msg)

    except Exception as e:
        print(e)



def publish_lines(producer, topic):
    # Read lines from stdin, produce each line to Kafka
    print("Type some lines... [ctrl-c] to exit.")
    for line in sys.stdin:
        try:
            # Produce line (without newline)
            producer.produce(topic, line.rstrip(), callback=acked)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        producer.poll(0)

def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started send_to_kafka')
    log.debug("------------------ Reading config ------------------")


    config = {'host': args.host, 'port': args.port, 'user': args.user, 'password': args.password}
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    _info = send_to_redis(config)

    print("Done.")
    logging.info('Finished send_to_kafka')
    exit_to_icinga(_info)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s '+version)

    parser.add_argument('-ho', '--host', help='Host', type=str, default="none", required=True)
    parser.add_argument('-p', '--port', help='Port', type=str, default="none", required=True)
    parser.add_argument('-u', '--user', help='User (default=None)', type=str, default=None)
    parser.add_argument('-pw', '--password', help='Password (default=None)', type=str, default=None)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)

