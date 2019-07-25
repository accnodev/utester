#!/usr/bin/env python3
# # -*- coding: utf-8 -*-
import os
import subprocess
import sys
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
    # subprocess.run executes a command and waits for it to finish
    # shell=True allows to execute pipes.

    # Important note about shell=True: On POSIX with shell=True, the shell defaults to /bin/sh.
    # If the command is a string, the string specifies the command to execute through the shell, so the string must be formatted exactly as it
    # would be when typed at the shell prompt. This includes, for example, quoting or backslash escaping filenames with spaces in them.
    # So, with shell=True, command must be a String, and with shell=False, command must be a list of Strings.
    return subprocess.run(command, stdout=subprocess.PIPE, shell=True).stdout.decode('utf-8')


def execute_shell_command_and_return_stdout_as_lines_list(command: str) -> List[str]:
    """
    Executes a shell command and returns it's stdout as a list, where each element of the list is a line of the stdout of the command.

    :param command: Command to be executed.
    :return: Stdout of the command, as a list of lines.
    """
    return execute_shell_command_and_return_stdout(command).split('\n')


# WARN: The block-device-mapping and the key options print it's content in more than one line, so with this option, dummy=True doesn't work correctly
def execute_ec2_metadata_command_and_return_stdout(option: str = None, ec2_dummy_file_relative_path: str = None) -> str:
    """
    If the ec2_dummy_file_relative_path parameter is None, executes the command ec2-metadata and returns it's stdout. \
    If the ec2_dummy_file_relative_path parameter is not None, simulates the stdout of the ec2-metadata, reading it's content from a file.

    :param option: Option passed to the ec2-metadata command. When it's dummy, this option must be specified with it's extended format, \
    for example, instead of '-a', use '--ami-id'.
    :param ec2_dummy_file_relative_path: Relative path to the file that simulates the stdout of the ec2-metadata command.
    :return: The ec2-metadata stdout (or it's simulated stdout, when the ec2_dummy_file_relative_path parameter is not None).
    """
    dummy = ec2_dummy_file_relative_path is not None

    if dummy:
        # Simulate the command stdout, reading it from a file
        with open(os.path.realpath(ec2_dummy_file_relative_path)) as ec2_dummy_file:
            ec2_dummy_result = ec2_dummy_file.read()

            if option is None:
                return ec2_dummy_result
            else:
                if not option.startswith('--'):
                    raise ValueError("When called with ec2_dummy_file_relative_path not None, the option must be specified with it's extended format,"
                                     " for example, instead of '-a', use '--ami-id'")

                # Return only the line that starts with the option passed
                ec2_dummy_result = ec2_dummy_result.split('\n')
                try:
                    return next(filter(lambda line: line.startswith(option[2:] + ':'), ec2_dummy_result))
                except StopIteration:
                    raise ValueError("The option {} is not valid.".format(option))

    else:
        # Execute the real command
        command = "ec2-metadata" if option is None else "ec2-metadata " + option
        return execute_shell_command_and_return_stdout(command)


def ok_message(message):
    """
    Shows the message with an OK format.
    """
    print("[OK] " + message)


def error_message(message):
    """
    Shows the message with an ERROR format.
    """
    sys.stderr.write("[ERROR] " + message + "\n")


def info_message(message):
    """
    Shows the message with an INFO format.
    """
    print("[INFO] " + message)