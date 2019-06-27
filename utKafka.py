#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""
Unit test a Kafka deployment.
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
import os
import sys

from pathlib import Path
from argparse import RawTextHelpFormatter

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
from helpers.utils import *
from helpers.kafka import *

log = logging.getLogger(Path(__file__).stem)
logfile = 'operations.log'
version = "1.0"


def send_to_kafka(config):
    log.debug("------------------ Begin send_to_kafka ------------------")
    log_trace = 'None'
    status = 'Ok'

    try:
        conf = {'bootstrap.servers': config['broker']}
        producer = Producer(**conf)

    except Exception as ex:
        e, _, ex_traceback = sys.exc_info()
        log_traceback(log, ex, ex_traceback)
        return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}

    # ------------------------- Switch options ------------------------- #
    if config['producelines']:
        publish_lines(producer, config['topic'])

    if config['listtopics']:
        a = create_admin_client(config['broker'])
        list_topics(a)

    if config['deletetopic']:
        a = create_admin_client(config['broker'])
        delete_topic(a, config['topic'])

    if config['describe'] != 'unknown':
        a = create_admin_client(config['broker'])
        describe_configs(a, config['describe'], config['configfilter'])
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End send_to_kafka ------------------")
    return {"logtrace": log_trace, "status": status}



def describe_configs(a, config, filter):
    """ describe configs
    Resource types:
    UNKNOWN = RESOURCE_UNKNOWN #: Resource type is not known or not set.
    ANY     = RESOURCE_ANY     #: Match any resource, used for lookups.
    TOPIC   = RESOURCE_TOPIC   #: Topic resource. Resource name is topic name
    GROUP   = RESOURCE_GROUP   #: Group resource. Resource name is group.id
    BROKER  = RESOURCE_BROKER  #: Broker resource. Resource name is broker id
    """

    # resources = [ConfigResource(restype, resname) for
    #              restype, resname in zip(args[0::2], args[1::2])]
    resources = [ConfigResource(config, filter)]

    fs = a.describe_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                print_config(config, 1)

        except KafkaException as e:
            print("Failed to describe {}: {}".format(res, e))
        except Exception:
            raise


def list_topics(a):
    topic_list = a.list_topics()
    print(topic_list.topics)


def delete_topic(a, topic):
    topics = [topic]
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))


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

    print("go out")


def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started send_to_kafka')
    log.debug("------------------ Reading config ------------------")


    config = {'broker': args.broker, 'topic': args.topic,
              'producelines': args.producelines,
              'listtopics': args.listtopics,
              'deletetopic': args.deletetopic,
              'describe': args.describe,
              'configfilter': args.configfilter
              }
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    nodes_info = send_to_kafka(config)

    print("Done.")
    logging.info('Finished send_to_kafka')
    exit_to_icinga(nodes_info)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s '+version)

    parser.add_argument('-b', '--broker', help='Broker', type=str, default="none", required=True)
    parser.add_argument('-t', '--topic', help='Topic (default=utester)', type=str, default="utester")
    parser.add_argument('-pl', '--producelines', help='Produce from command line inputs', action='store_const', const=True, default=False)
    parser.add_argument('-lt', '--listtopics', help='List Topics', action='store_const', const=True, default=False)
    parser.add_argument('-dt', '--deletetopic', help='Delete Topic (if topic not specified, default topic will be deleted)', action='store_const', const=True, default=False)
    # parser.add_argument('-sc', '--showconfig', help='Show Config', action='store_const', const=True, default=False)
    parser.add_argument('-d', '--describe', default='unknown', const='all', nargs='?', choices=['unknown', 'any', 'topic', 'group', 'broker'], help='Resource type from list (default: %(default)s)')
    # parser.add_argument('filter', metavar='N', type=str, nargs='+', help='an integer for the accumulator')
    parser.add_argument('-cf', '--configfilter', type=str, help='A value to filter Resources. Required if ShowConfig is present', required='-sc' in sys.argv)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)

