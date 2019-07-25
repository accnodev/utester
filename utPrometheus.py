#!/usr/bin/env python3
# # -*- coding: utf-8 -*-

"""
Send metrics from Python to a file with the Prometheus format.
Metric types:
 - Counter.
 - Gauge.
 - Histogram.
 - Summary.

Example:
    Send a Counter metric to a file
        python utPrometheus.py -f /path/to/file -mn metricName -md metricDescription --counter 2.5
    Send a Gauge metric to a file
        python utPrometheus.py -f /path/to/file -mn metricName -md metricDescription --gauge 5.5
    Send a Histogram metric to a file
        python utPrometheus.py -f /path/to/file -mn metricName -md metricDescription --histogram 2
    Send a Summary metric to a file
        python utPrometheus.py -f /path/to/file -mn metricName -md metricDescription --summary 3
    Send all metrics to a file
        python utPrometheus.py -f /path/to/file -mn metricName -md metricDescription -c 2.5 -g 5.5 -hi 2 -s 3

"""

import argparse
import logging
import time
from argparse import RawTextHelpFormatter

from prometheus_client import CollectorRegistry, Gauge, write_to_textfile, Counter, Histogram, Summary

from helpers.utils import *

log = logging.getLogger(os.path.splitext(__file__)[0])
logfile = 'operations.log'
version = "1.0"


def emit_metric(config):
    log.debug("------------------ Begin emit_metric ------------------")
    log_trace = 'None'
    status = 'Ok'

    # Create registry to collect the metric. A separate registry is used,
    # as the default registry may contain other metrics such as those from the Process Collector.
    registry = CollectorRegistry()

    # ------------------------- Switch options ------------------------- #
    # At least one option must be passed
    if not (config['counter'] or config['gauge'] or config['histogram'] or config['summary']):
        error_message("At least one of this options must be passed: -c/--counter, -g/--gauge, -hi/--histogram, -s/--summary")

    # In this case, all options can be used at the same time
    if config['counter']:
        emit_counter_metric(registry, config['metricname'], config['metricdescription'], config['counter'])
    if config['gauge']:
        emit_gauge_metric(registry, config['metricname'], config['metricdescription'], config['gauge'])
    if config['histogram']:
        emit_histogram_metric(registry, config['metricname'], config['metricdescription'], config['histogram'])
    if config['summary']:
        emit_summary_metric(registry, config['metricname'], config['metricdescription'], config['summary'])
    # ------------------------------------------------------------------ #

    # Send the metrics to the specified file
    file_path: str = config['file']
    write_to_textfile(file_path, registry)

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End emit_metric ------------------")
    return {"logtrace": log_trace, "status": status}


def emit_counter_metric(registry: CollectorRegistry, metric_name: str, metric_description: str, increment_value: float):
    """
    Emits a metric of type Counter, incrementing it's initial value (0.0) with the given value.
    """
    try:
        # Add suffix to the metric name and prefix to the metric description
        metric_name = metric_name + "Counter"
        metric_description = "Counter metric description: " + metric_description

        counter = Counter(metric_name, metric_description, registry=registry)
        counter.inc(increment_value)
        ok_message("Counter metric '{}' incremented in value '{}'".format(metric_name, increment_value))
    except Exception as error:
        error_message("Error while emitting Counter metric: {}".format(error))


def emit_gauge_metric(registry: CollectorRegistry, metric_name: str, metric_description: str, value: float):
    """
    Emits a metric of type Gauge, with the given value.
    """
    try:
        # Add suffix to the metric name and prefix to the metric description
        metric_name = metric_name + "Gauge"
        metric_description = "Gauge metric description: " + metric_description

        gauge = Gauge(metric_name, metric_description, registry=registry)
        gauge.set(value)
        ok_message("Gauge metric '{}' value setted to '{}'".format(metric_name, value))
    except Exception as error:
        error_message("Error while emitting Gauge metric: {}".format(error))


def emit_histogram_metric(registry: CollectorRegistry, metric_name: str, metric_description: str, seconds: float):
    """
    Emits a metric of type Histogram, that takes into account the number of times a function is called in a period of time.
    """
    try:
        # Add suffix to the metric name and prefix to the metric description
        metric_name = metric_name + "Histogram"
        metric_description = "Histogram metric description: " + metric_description

        histogram = Histogram(metric_name, metric_description, registry=registry)
        histogram.observe(seconds)

        @histogram.time()
        def dummy_function_with_sleep(seconds):
            """A dummy function"""
            time.sleep(seconds)

        dummy_function_with_sleep(0.1)
        dummy_function_with_sleep(0.2)
        dummy_function_with_sleep(0.3)
        dummy_function_with_sleep(0.2)
        dummy_function_with_sleep(0.1)

        ok_message("Histogram metric '{}' was created".format(metric_name))
    except Exception as error:
        error_message("Error while emitting Histogram metric: {}".format(error))


def emit_summary_metric(registry: CollectorRegistry, metric_name: str, metric_description: str, seconds: float):
    """
    Emits a metric of type Summary, that takes into account the number of times a function is called in a period of time.
    """
    try:
        # Add suffix to the metric name and prefix to the metric description
        metric_name = metric_name + "Summary"
        metric_description = "Summary metric description: " + metric_description

        summary = Summary(metric_name, metric_description, registry=registry)
        summary.observe(seconds)

        @summary.time()
        def dummy_function_with_sleep(seconds):
            """A dummy function"""
            time.sleep(seconds)

        dummy_function_with_sleep(0.1)
        dummy_function_with_sleep(0.2)
        dummy_function_with_sleep(0.3)
        dummy_function_with_sleep(0.2)
        dummy_function_with_sleep(0.1)

        ok_message("Summary metric '{}' was created".format(metric_name))
    except Exception as error:
        error_message("Error while emitting Summary metric: {}".format(error))


def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started emit_metric')
    log.debug("------------------ Reading config ------------------")

    config = {
        'file': args.file,
        'metricname': args.metricname,
        'metricdescription': args.metricdescription,
        'counter': args.counter,
        'gauge': args.gauge,
        'histogram': args.histogram,
        'summary': args.summary,
    }
    config['root_dir'] = os.path.dirname(os.path.abspath(__file__))

    _info = emit_metric(config)

    print("Done.")
    logging.info('Finished emit_metric')
    exit_to_icinga(_info)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)

    parser.add_argument('-f', '--file', help='Path to the file where the metrics will be saved', type=str, default=None, required=True)
    parser.add_argument('-mn', '--metricname', help='Metric name', type=str, default=None, required=True)
    parser.add_argument('-md', '--metricdescription', help='Metric description', type=str, default=None, required=True)

    parser.add_argument('-c', '--counter', help='Emit a metric of type Counter. The value of this param is the value used to increment the counter.',
                        type=float, default=None)
    parser.add_argument('-g', '--gauge', help='Emit a metric of type Gauge. The value of this param is the value of the gauge metric.',
                        type=float, default=None)
    parser.add_argument('-hi', '--histogram',
                        help='Emit a metric of type Histogram. The value of this param is the number of seconds to observe the dummy method.',
                        type=float, default=None)
    parser.add_argument('-s', '--summary',
                        help='Emit a metric of type Summary. The value of this param is the number of seconds to observe the dummy method.',
                        type=float, default=None)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)
