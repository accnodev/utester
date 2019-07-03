#!/usr/bin/env python
# # -*- coding: utf-8 -*-
import subprocess
import sys
import os
import traceback

# from sys import exit
# from requests.exceptions import RequestException
from typing import List


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


def execute_shell_command_and_return_stdout(command: str) -> str:
    """
    Executes a shell command and returns it's stdout.

    :param command: Command to be executed.
    :return: Stdout of the command.
    """
    return subprocess.run(command.split(), stdout=subprocess.PIPE).stdout.decode('utf-8')


def execute_shell_command_and_return_stdout_as_lines_list(command: str) -> List[str]:
    """
    Executes a shell command and returns it's stdout as a list, where each element of the list is a line of the stdout of the command.

    :param command: Command to be executed.
    :return: Stdout of the command, as a list of lines.
    """
    return execute_shell_command_and_return_stdout(command).split('\n')


# TODO: The block-device-mapping and the key options prints it's content in more than one line, so with this option, dummy=True doesn't work correctly
def execute_ec2_metadata_command_and_return_stdout(option: str = None, dummy=False, ec2_dummy_file_relative_path: str = None) -> str:
    """
    If the dummy parameter is False, executes the command ec2-metadata and returns it's stdout. \
    If is True, simulates the stdout of the ec2-metadata, reading it's content from a file.

    :param option: Option passed to the ec2-metadata command. When it's dummy, this option must be specified with it's extended format, \
    for example, instead of '-a', use '--ami-id'.
    :param dummy: Specifies if the command is really executed or it's stdout is simulated, reading it from a file.
    :param ec2_dummy_file_relative_path: Relative path to the file that simulates the stdout of the ec2-metadata command. Only used if dummy is True.
    :return: The ec2-metadata stdout (or it's simulated stdout, when dummy is True).
    """
    if dummy:
        # Simulate the command stdout, reading it from a file
        with open(os.path.realpath(ec2_dummy_file_relative_path)) as ec2_dummy_file:
            ec2_dummy_result = ec2_dummy_file.read()

            if option is None:
                return ec2_dummy_result
            else:
                if not option.startswith('--'):
                    raise ValueError("When called with dummy=True, the option must be specified with it's extended format, "
                                     "for example, instead of '-a', use '--ami-id'")

                # Return only the line that starts with the option passed
                try:
                    return next(filter(lambda line: line.startswith(option[2:]), ec2_dummy_result))
                except StopIteration:
                    raise ValueError("The option {} is not valid.".format(option))

    else:
        # Execute the real command
        command = "ec2-metadata" if option is None else "ec2-metadata " + option
        return execute_shell_command_and_return_stdout(command)
