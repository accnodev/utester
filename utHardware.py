#!/usr/bin/env python
# # -*- coding: utf-8 -*-

"""
Unit test the hardware of a machine.
Possible machine types:
 - bastion
 - kafka
 - striim
 - psql
 - emr
 - redis
 - psql2
 - dns: This type checks that the fqdns are resolved by the DNS. Must be executed from bastion.

If this file is NOT executed in an EC2 instance, the --ec2-dummy must be used,
pointing to a file that simulates the ec2-metadata command.

Examples:
    Check an EC2 kafka machine
        python utHardware.py --configfile config/config.global.json --type kafka

    Check a kafka machine that is NOT an EC2 instance (for testing purposes, for example)
        python utHardware.py --configfile config/config.global.json --type kafka --dummy config/ec2-metadata-dummy.global.txt

    Check that the fqdns of the rest of machines are resolved by the DNS, from the EC2 bastion machine
        python utHardware.py --configfile config/config.global.json --type dns

"""

import argparse
import logging
import json
import urllib.request
import urllib.error

from argparse import RawTextHelpFormatter
from typing import Dict

import psutil

from helpers.utils import *

log = logging.getLogger(os.path.splitext(__file__)[0])
logfile = 'operations.log'
version = "1.0"


def check_hardware(config: Dict):
    log.debug("------------------ Begin check_hardware ------------------")
    log_trace = 'None'
    status = 'Ok'

    # Obtain the configuration for the machine type specified in the CLI arguments
    machine_type = config['type']
    machine_uthardware_config = next(filter(lambda machine_conf: machine_conf['type'] == machine_type, config['uthardwareconfig']))
    assert type(machine_uthardware_config) == dict
    assert machine_uthardware_config['type'] == machine_type

    # Obtain the ec2-dummy file relative path (is None if not specified in the CLI by the user)
    ec2_dummy_path: str = config['ec2_dummy']

    # Obtain the fully qualified domain name
    fqdn: str = execute_ec2_metadata_command_and_return_stdout("--local-hostname", ec2_dummy_path).split()[1]

    # ------------------------- Switch options ------------------------- #
    if machine_type == 'bastion':
        check_bastion(machine_uthardware_config)

    elif machine_type == 'kafka':
        check_kafka(machine_uthardware_config, fqdn, ec2_dummy_path)

    elif machine_type == 'striim':
        check_striim(machine_uthardware_config, fqdn, ec2_dummy_path)

    elif machine_type == 'psql':
        check_psql(machine_uthardware_config, fqdn, ec2_dummy_path)

    elif machine_type == 'emr':
        check_emr(machine_uthardware_config, fqdn, ec2_dummy_path)

    elif machine_type == 'redis':
        check_redis(machine_uthardware_config, ec2_dummy_path)

    elif machine_type == 'psql2':
        check_psql2(machine_uthardware_config, ec2_dummy_path)

    elif machine_type == 'dns':
        check_dns(machine_uthardware_config)
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End check_hardware ------------------")
    return {"logtrace": log_trace, "status": status}


# ---------------- BEGIN Machine checks ---------------- #

def check_bastion(config: Dict):
    check_ingress(config['hardware']['ingress'])


def check_kafka(config: Dict, fqdn: str, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_fs(config['hardware']['fs'])
    # DNS is tested from bastion
    check_ingress(config['hardware']['ingress'])
    check_etc_hosts(fqdn)
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_striim(config: Dict, fqdn: str, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_fs(config['hardware']['fs'])
    # DNS is tested from bastion
    check_ingress(config['hardware']['ingress'])
    check_etc_hosts(fqdn)
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_psql(config: Dict, fqdn: str, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_fs(config['hardware']['fs'])
    # DNS is tested from bastion
    check_ingress(config['hardware']['ingress'])
    check_etc_hosts(fqdn)
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_emr(config: Dict, fqdn: str, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_fs(config['hardware']['fs'])
    # DNS is tested from bastion
    check_ingress(config['hardware']['ingress'])
    check_etc_hosts(fqdn)
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_redis(config: Dict, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_ingress(config['hardware']['ingress'])
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_psql2(config: Dict, ec2_dummy_path: str):
    check_instance_type(config['hardware']['instance_type'], ec2_dummy_path)

    check_ingress(config['hardware']['ingress'])
    check_certs(config['hardware']['certs'])
    check_tz(config['hardware']['tz'])


def check_dns(config: Dict):
    """
    Checks if the Fully Qualified Domain Names of the rest of machines are resolved by the DNS.
    """
    fqdns: List[str] = config['fqdns']

    # For each fqdn, check if the dig command has the answer section.
    # If the fqdn is not resolved, the answer section doesn't appear (authority section appears instead).
    for fqdn in fqdns:
        dig_result = execute_shell_command_and_return_stdout_as_lines_list("dig " + fqdn)
        try:
            next(filter(lambda line: line.startswith(";; ANSWER SECTION:"), dig_result))
            # Here, the answer section appeared
            ok_message("The fqdn '{}' is resolved by the DNS.".format(fqdn))
        except StopIteration:
            # If the filter object is empty, the answer section didn't appear
            error_message("The fqdn '{}' is NOT resolved by the DNS.\n".format(fqdn))


# ---------------- END Machine checks ---------------- #

# ---------------- BEGIN Unit checks ---------------- #


def check_fs(required_mountpoints: List[str]):
    """
    Checks that the required mountpoints exist in distinct partitions.
    :param required_mountpoints: Required mountpoints, obtained from the configuration file.
    """
    mountpoint_partitions = set()

    for required_mountpoint in required_mountpoints:
        try:
            # Obtain the disk partition where the required mountpoint is mounted (if exists)
            mounted_disk_partition = next(filter(lambda dp: dp.mountpoint == required_mountpoint, psutil.disk_partitions()))
            # If the mountpoint is not mounted, a StopIteration exception is raised

            # Check that the required mountpoint isn't mounted in the same partition as another required mountpoint
            if mounted_disk_partition.device in mountpoint_partitions:
                error_message("The required mountpoint {} is mounted in the same partition ({}) as another required mountpoint.\n"
                              .format(required_mountpoint, mounted_disk_partition.device))
            else:
                ok_message("The required mountpoint {} is correctly mounted."
                           .format(required_mountpoint, mounted_disk_partition.device))
                mountpoint_partitions.add(mounted_disk_partition.device)
        except StopIteration:
            error_message("The required mountpoint {} is not mounted.\n".format(required_mountpoint))

    # Print the df command info
    info_message("Partitions size:")
    print(execute_shell_command_and_return_stdout("df -h --output=source,size,pcent"))


def check_ingress(required_opened_ports: List[int]):
    """
    Checks that the ingress ports are opened.
    :param required_opened_ports: Ports that are required to be opened, obtained from the configuration file.
    """
    netstat = execute_shell_command_and_return_stdout_as_lines_list("netstat -tunpl")[2:]  # Remove the first and the second lines

    # Remove empty lines
    netstat = filter(lambda line: len(line.strip()) > 0, netstat)
    # Transform the list of lines of the netstat command to a set of opened ports
    current_opened_ports = {line.split()[3].split(':')[-1] for line in netstat}  # The 'Local Address' column (the 4th) contains the ingress info

    # For each one of the required ports, check if it's present in the current opened ports set
    for required_opened_port in map(lambda x: str(x), required_opened_ports):
        if required_opened_port in current_opened_ports:
            ok_message("The port {} is opened.".format(required_opened_port))
        else:
            error_message("The port {} is NOT opened.\n".format(required_opened_port))


def check_etc_hosts(fqdn: str):
    """
    Checks that the /etc/hosts file contains a line that links the loopback IP with the FQDN (Fully Qualified Domain Name).
    :param fqdn: Fully Qualified Domain Name, obtained from the configuration file.
    """
    etc_hosts = execute_shell_command_and_return_stdout_as_lines_list("cat /etc/hosts")

    # Remove empty lines and comment lines
    etc_hosts = filter(lambda line: len(line) > 0 and line[0] != '#', etc_hosts)

    # Keep only the lines that refer to the loopback
    etc_hosts = filter(lambda line: line.split()[0] == "127.0.0.1", etc_hosts)

    # Check that the fqdn is in the second position of one of the remaining lines
    for line in etc_hosts:
        if line.split()[1] == fqdn:
            ok_message("etc/hosts file is correctly configured. Loopback IP is related to the FQDN.")
            return

    error_message("etc/hosts file isn't configured correctly. Loopback IP isn't related to the FQDN. "
                  "Add this line to the /etc/hosts file: '127.0.0.1    {}'\n".format(fqdn))


def check_instance_type(expected_instance_type: str, ec2_dummy_path: str):
    """
    Checks that the AWS instance type (m5.large, t2.micro, etc) is the expected.
    :param expected_instance_type: Expected AWS instance type, obtained from the configuration file.
    :param ec2_dummy_path: Relative path to the ec2-metadata dummy file, obtained from the configuration file.
    """
    instance_type = execute_ec2_metadata_command_and_return_stdout("--instance-type", ec2_dummy_path).split()[1]
    if instance_type == expected_instance_type:
        ok_message("Instance type is OK")
    else:
        error_message("Instance type is WRONG. Expected: {}. Current {}.\n"
                      .format(expected_instance_type, instance_type))


def check_certs(cert_path: str):
    """
    Checks that the certificates specified in the configuration file exist.
    :param cert_path: Path where the certificate file should be located, obtained from the configuration file.
    """
    if os.path.exists(cert_path):
        ok_message("Certificates exist")
    else:
        error_message("Certificates doesn't exist in this path: {}.\n".format(cert_path))


def check_tz(expected_tz: str):
    """
    Checks that the timezone is the same as the one specified in the configuration file.
    :param expected_tz: Value of the expected timezone, obtained from the configuration file.
    """
    tz = execute_shell_command_and_return_stdout('timedatectl | grep "Time zone"').split("Time zone:")[1].split()[0]
    if tz == expected_tz:
        ok_message("Timezone is OK")
    else:
        error_message("Timezone is WRONG. Expected: {}. Current: {}.\n".format(expected_tz, tz))


# TODO: This method is not currently used. In which machine is this method executed?
def check_ntpd():
    """
    Checks that the ntpd.service is active and running.
    """
    try:
        ntpd_status = execute_shell_command_and_return_stdout('systemctl status ntpd | grep "Active:"').split("Active:")[1].strip()

        if ntpd_status.startswith("active (running)"):
            ok_message("ntpd is active and running")
        else:
            error_message("ntpd is not active and running. Current ntpd status: {}.\n".format(ntpd_status))
    except IndexError:
        # An IndexError can be raised by 2 reasons:
        # * The ntpd.service could not be found by the systemctl status command
        # * The grep command didn't find "Active:"
        # This means that ntpd is not configured correctly
        error_message("ntpd.service could not be found.\n")


# TODO: This method is not currently used. In which machine is this method executed? bastion?
def check_config_metrics(fqdn: str, metrics_endpoints: List[str]):
    """
    Checks that the metrics endpoints are running.
    :param fqdn: Fully Qualified Domain Name, obtained from the configuration file.
    :param metrics_endpoints: List of metrics endpoints that must be running, obtained from the configuration file.
    """
    for metrics_endpoint in metrics_endpoints:
        metrics_endpoint_with_fqdn = "http://" + metrics_endpoint.replace('<fqdn>', fqdn)

        try:
            # Make an HTTP GET request to the endpoint
            response_code = urllib.request.urlopen(metrics_endpoint_with_fqdn).getcode()

            if response_code == 200:
                ok_message("The metrics endpoint {} is running.".format(metrics_endpoint_with_fqdn))
            else:
                error_message("The metrics endpoint {} response wasn't 200 OK. HTTP status code: {}.\n"
                              .format(metrics_endpoint_with_fqdn, response_code))

        except urllib.error.URLError as e:
            # If the endpoint is not running, an exception is raised
            error_message("The metrics endpoint {} is not running. Reason: {}.\n".format(metrics_endpoint_with_fqdn, e.reason))


# ---------------- END Unit checks ---------------- #

def main(args, loglevel):
    if args.logging:
        logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel)
    logging.info('Started check_hardware')
    log.debug("------------------ Reading config ------------------")

    # Parse the configfile
    configfile_json = json.load(open(os.path.realpath(args.configfile)))
    # Obtain the JSON object with the utHardware configuration
    uthardwareconfig = configfile_json['utHardware']

    config = {
        'uthardwareconfig': uthardwareconfig,
        'type': args.type,
        'ec2_dummy': args.ec2_dummy,
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

    parser.add_argument('-c', '--configfile', help='Configuration file path (.json).', type=str, default=None, required=True)
    parser.add_argument('-t', '--type', help='Machine type (i.e. kafka). Use dns to check DNS from bastion.', type=str, default=None, required=True)
    parser.add_argument('-d', '--ec2-dummy', help='Path to the file that contains the simulation of the ec2-metadata command stdout.'
                                                  'If present, the ec2-metadata stdout will be simulated, using the file passed to this option.',
                        type=str, default=None, required=False)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)
