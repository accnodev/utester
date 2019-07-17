#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
Unit test a PostgreSQL deployment.
Functionalities:
 - Connect with ssl.
 - Connect without ssl.
 - Get Version. Test connection with PostgreSQL and get version.
 - Count rows in a table..


In order to work with password, store it under /root/psa/.psa.shadow.

Example:
    Connect using SSL
        python utPostgre.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl
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

import psycopg2

from helpers.utils import *

log = logging.getLogger(os.path.splitext(__file__)[0])
logfile = 'operations.log'
version = "1.0"


def check_postgre(config):
    log.debug("------------------ Begin send_to_redis ------------------")
    log_trace = 'None'
    status = 'Ok'

    # ------------------------- Switch options ------------------------- #
    # SSL connection or not
    if config['sslconnection']:
        postgre = connect_postgre_with_ssl(config)
    else:
        postgre = connect_postgre_without_ssl(config)

    # Options
    if config['getversion']:
        get_version(postgre)

    elif config['counttable']:
        count_table(postgre, config['counttable'])
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End send_to_redis ------------------")
    return {"logtrace": log_trace, "status": status}


def connect_postgre_without_ssl(config):
    try:
        conn = psycopg2.connect(user=config['user'], password=config['password'], host=config['host'], port=config['port'], dbname=config['dbname'])
        print('Connected!')
    except Exception as ex:
        e, _, ex_traceback = sys.exc_info()
        log_traceback(log, ex, ex_traceback)
        return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}
    return conn


def connect_postgre_with_ssl(config):
    try:
        conn = psycopg2.connect(user=config['user'], password=config['password'], host=config['host'], port=config['port'], dbname=config['dbname'], sslmode='require')
        print('Connected!')
    except Exception as ex:
        e, _, ex_traceback = sys.exc_info()
        log_traceback(log, ex, ex_traceback)
        return {"logtrace": "HOST UNREACHABLE", "status": "UNKNOWN"}
    return conn


def count_table(connection, table):
    try:
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        # Print PostgreSQL version
        cursor.execute("SELECT count(*) FROM "+table+";")
        record = cursor.fetchone()
        print("Number of rows in ", table, ": ", record[0],"\n")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def get_version(connection):
    try:
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started send_to_kafka')
    log.debug("------------------ Reading config ------------------")

    config = {
        'host': args.host, 'port': args.port, 'user': args.user, 'password': args.password, 'dbname': args.dbname,
        'getversion': args.getversion,
        'sslconnection': args.sslconnection,
        'counttable': args.counttable,
    }
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    _info = check_postgre(config)

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
    parser.add_argument('-db', '--dbname', help='DataBase name', type=str, required=True)
    parser.add_argument('-ssl', '--sslconnection', help='Use SSL connection', action='store_const', const=True, default=False)

    parser.add_argument('-gv', '--getversion', help='Get Postgre Version', action='store_const', const=True, default=False)
    parser.add_argument('-c', '--counttable', help='Count number of rows in a table', type=str, default=None)


    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)