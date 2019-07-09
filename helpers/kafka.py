#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
    File name: helpers.kafka.py
    Author: Agustín Calderón.
    Date created: 26/06/2019
    Date last modified: 26/06/2019
    Python Version: 3.7.2
    Version: 1.0.0
"""

import sys

from confluent_kafka.admin import AdminClient, ConfigSource


def acked(error, message):
    if error:
        sys.stderr.write('%% Message failed delivery: %s\n' % error)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (message.topic(), message.partition(), message.offset()))
        print("------------------ delivery callback ------------------")
        print("error={}.".format(error))
        print("message.topic={}".format(message.topic()))
        print("message.timestamp={}".format(message.timestamp()))
        print("message.key={}".format(message.key()))
        print("message.partition={}".format(message.partition()))
        print("message.offset={}".format(message.offset()))


def create_admin_client(broker):
    # Create Admin client
    a = AdminClient({'bootstrap.servers': broker})
    return a


def print_config(config, depth):
    print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))
