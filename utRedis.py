#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""
Unit test a Redis deployment.
Functionalities:
 - Connect with ssl.
 - Connect without ssl.
 - Hello test. Send Hello message to Redis and get it back.
 - Get all keys.
 - Flush all.
 - Get by ky. Get message by key.
 - Delete key (or keys).

In order to work with password, store it under /root/psa/.psa.shadow.

Example:
    Connect using SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl
    Connect without SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow`
    Hello test with SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -ht
    Get all keys
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -ak
    Flush all (delete all keys in all databases) with SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -fa
    Get key with SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -gk msg:hello
    Delete key (or keys, separated by spaces) with SSL
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -dk key1
        python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -dk key1 key2 key3

"""

import argparse
import logging

from argparse import RawTextHelpFormatter

import redis
from helpers.utils import *

log = logging.getLogger(os.path.splitext(__file__)[0])
logfile = 'operations.log'
version = "1.0"


def send_to_redis(config):
    log.debug("------------------ Begin send_to_redis ------------------")
    log_trace = 'None'
    status = 'Ok'

    # ------------------------- Switch options ------------------------- #
    # SSL connection or not
    if config['sslconnection']:
        redis = connect_redis_with_ssl(config)
    else:
        redis = connect_redis_without_ssl(config)

    # Options
    if config['hellotest']:
        hello_redis(redis)

    elif config['allkeys']:
        get_all_keys(redis)

    elif config['flushall']:
        flush_all(redis)

    elif config['getkey']:
        get_key(redis, config['getkey'])

    elif config['delkey']:
        delete_keys(redis, config['delkey'])
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End send_to_redis ------------------")
    return {"logtrace": log_trace, "status": status}


def connect_redis_without_ssl(config):
    try:
        conn = redis.StrictRedis(host=config['host'], port=config['port'], password=config['password'])
        print(conn)
        conn.ping()
        print('Connected!')
    except Exception as ex:
        e, _, ex_traceback = sys.exc_info()
        log_traceback(log, ex, ex_traceback)
        return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}
    return conn


def connect_redis_with_ssl(config):
    try:
        conn = redis.StrictRedis(host=config['host'], port=config['port'], password=config['password'], ssl=True)
        # conn = redis.StrictRedis(host=config['host'], port=config['port'], ssl=True,
        #                          ssl_ca_certs='/etc/pki/tls/certs/cacertorange.crt')
        print(conn)
        conn.ping()
        print('Connected!')
    except Exception as ex:
        e, _, ex_traceback = sys.exc_info()
        log_traceback(log, ex, ex_traceback)
        return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}
    return conn


def get_all_keys(redis: redis.Redis):
    for key in redis.scan_iter():
        print(key)


def flush_all(redis: redis.Redis):
    """
    Delete all keys in all databases.
    """
    redis.flushall()


def get_key(redis: redis.Redis, key):
    try:
        msg = redis.get(key)
        print("msg:", str(msg))
    except Exception as e:
        error_message(e)


def delete_keys(redis: redis.Redis, keys: List[str]):
    try:
        redis.delete(*keys)
    except Exception as e:
        error_message(e)


def hello_redis(redis):
    try:
        # step 1: Set the hello message in Redis
        redis.set("msg:hello", "Hello Redis!!!")

        # step 2: Retrieve the hello message from Redis
        msg = redis.get("msg:hello")
        print("msg:", str(msg))
    except Exception as e:
        error_message(e)


def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started send_to_kafka')
    log.debug("------------------ Reading config ------------------")

    config = {
        'host': args.host, 'port': args.port, 'user': args.user, 'password': args.password,
        'hellotest': args.hellotest,
        'sslconnection': args.sslconnection,
        'allkeys': args.allkeys,
        'flushall': args.flushall,
        'getkey': args.getkey,
        'delkey': args.delkey,
    }
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    _info = send_to_redis(config)

    print("Done.")
    logging.info('Finished send_to_kafka')
    exit_to_icinga(_info)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)

    parser.add_argument('-ho', '--host', help='Host', type=str, default="none", required=True)
    parser.add_argument('-p', '--port', help='Port', type=str, default="6379", required=False)
    parser.add_argument('-u', '--user', help='User (default=None)', type=str, default=None)
    parser.add_argument('-pw', '--password', help='Password (default=None)', type=str, default=None)

    parser.add_argument('-ssl', '--sslconnection', help='Use SSL connection', action='store_const', const=True, default=False)
    parser.add_argument('-ht', '--hellotest', help='Hello test', action='store_const', const=True, default=False)
    parser.add_argument('-ak', '--allkeys', help='Show all keys', action='store_const', const=True, default=None)
    parser.add_argument('-fa', '--flushall', help='Delete all keys in all databases', action='store_const', const=True, default=None)
    parser.add_argument('-gk', '--getkey', help='Get by key value (default=None)', type=str, default=None)
    parser.add_argument('-dk', '--delkey', help='Delete key (or keys, separated by spaces)', nargs='+', type=str, default=None)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)
