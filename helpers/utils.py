#!/usr/bin/env python
# # -*- coding: utf-8 -*-
import subprocess
import sys
import os
import traceback


# from sys import exit
# from requests.exceptions import RequestException


def setPath(namespace):
    root_path = os.path.dirname(__file__).replace(namespace, '')
    paths = [root_path]
    for p in paths:
        sys.path.insert(0, p)


def log_traceback(log, ex, ex_traceback=None):
    if ex_traceback is None:
        ex_traceback = ex.__traceback__
    tb_lines = [line.rstrip('\n') for line in
                traceback.format_exception(ex.__class__, ex, ex_traceback)]
    log.error(tb_lines)


def exit_to_icinga(nodes_info):
    print(nodes_info['logtrace'])
    if nodes_info['status'] == "OK":
        exit(0)
    elif nodes_info['status'] == "WARNING":
        exit(1)
    elif nodes_info['status'] == "CRITICAL":
        exit(2)
    else:
        exit(3)


def get_schema_path(fname):
    dname = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dname, fname)


def load_schema_file(fname):
    fname = get_schema_path(fname)
    with open(fname) as f:
        return f.read()


def execute_shell_command_and_return_stdout(command: str):
    return subprocess.run(command.split(), stdout=subprocess.PIPE).stdout.decode('utf-8')


def execute_shell_command_and_return_stdout_as_lines_array(command: str):
    return execute_shell_command_and_return_stdout(command).split('\n')