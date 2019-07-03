#!/usr/bin/env python
# # -*- coding: utf-8 -*-

# TODO: Modify the help message based on all the hardware checks
"""
Unit test the hardware of a machine.
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
import json
import urllib.request
import urllib.error

from argparse import RawTextHelpFormatter
from typing import Dict

import psutil

from helpers.utils import *

# from helpers.hardware import *

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

    if machine_type == 'kafka':
        check_kafka(machine_uthardware_config, fqdn, ec2_dummy_path)

    if machine_type == 'striim':
        check_striim(machine_uthardware_config, fqdn)

    if machine_type == 'psql':
        check_psql(machine_uthardware_config, fqdn)

    if machine_type == 'emr':
        check_emr(machine_uthardware_config, fqdn)

    if machine_type == 'redis':
        check_redis(machine_uthardware_config)

    # TODO: In the image there is another psql
    # ------------------------------------------------------------------ #

    log_trace = "Send " + status + " | " + log_trace
    log.debug("------------------ End check_hardware ------------------")
    return {"logtrace": log_trace, "status": status}


# ---------------- BEGIN Machine checks ---------------- #

def check_bastion(bastion_config: Dict):
    # TODO
    pass


def check_kafka(kafka_config: Dict, fqdn: str, ec2_dummy_path: str):
    check_instance_type(kafka_config['hardware']['instance_type'], ec2_dummy_path)

    check_fs(kafka_config['hardware']['fs'])
    # TODO: check_dns()
    check_ingress(kafka_config['hardware']['ingress'])
    # TODO: check_egress()
    check_etc_hosts(fqdn)
    check_certs(kafka_config['hardware']['certs'])
    check_tz(kafka_config['hardware']['tz'])


def check_striim(striim_config: Dict, fqdn: str):
    # TODO
    pass


def check_psql(psql_config: Dict, fqdn: str):
    # TODO
    pass


def check_emr(emr_config: Dict, fqdn: str):
    # TODO
    pass


def check_redis(redis_config: Dict):
    # TODO
    pass


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
                sys.stderr.write("[ERROR] The required mountpoint {} is mounted in the same partition ({}) as another required mountpoint.\n"
                                 .format(required_mountpoint, mounted_disk_partition.device))
            else:
                print("The required mountpoint {} is correctly mounted."
                      .format(required_mountpoint, mounted_disk_partition.device))
                mountpoint_partitions.add(mounted_disk_partition.device)
        except StopIteration as e:
            sys.stderr.write("[ERROR] The required mountpoint {} is not mounted.\n".format(required_mountpoint))

    # Print the df command info
    print("\nPartitions size:")
    print(os.system("df -h --output=source,size,pcent"))


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
            print("The port {} is opened.".format(required_opened_port))
        else:
            sys.stderr.write("[ERROR] The port {} is NOT opened.\n".format(required_opened_port))


# TODO: If we have to check more than the loopback IP, then the config.global.json file needs to be used.
#  If not, the etchost attribute can be delated from the json.
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
            print("etc/hosts file is correctly configured. Loopback IP is related to the FQDN.")
            return

    sys.stderr.write("[ERROR] etc/hosts file isn't configured correctly. Loopback IP isn't related to the FQDN. "
                     "Add this line to the /etc/hosts file: '127.0.0.1    {}'\n".format(fqdn))


def check_instance_type(expected_instance_type: str, ec2_dummy_path: str):
    """
    Checks that the AWS instance type (m5.large, t2.micro, etc) is the expected.
    :param expected_instance_type: Expected AWS instance type, obtained from the configuration file.
    :param ec2_dummy_path: Relative path to the ec2-metadata dummy file, obtained from the configuration file.
    """
    instance_type = execute_ec2_metadata_command_and_return_stdout("--instance-type", ec2_dummy_path).split()[1]
    if instance_type == expected_instance_type:
        print("Instance type is OK")
    else:
        sys.stderr.write("[ERROR] Instance type is WRONG. Expected: {}. Current {}.\n"
                         .format(expected_instance_type, instance_type))


def check_certs(cert_path: str):
    """
    Checks that the certificates specified in the configuration file exist.
    :param cert_path: Path where the certificate file should be located, obtained from the configuration file.
    """
    # TODO: Is there only one certificate, or we need to check more than one file?
    if os.path.exists(cert_path):
        print("Certificates exist")
    else:
        sys.stderr.write("[ERROR] Certificates doesn't exist in this path: {}.\n".format(cert_path))


def check_tz(expected_tz: str):
    """
    Checks that the timezone is the same as the one specified in the configuration file.
    :param expected_tz: Value of the expected timezone, obtained from the configuration file.
    """
    tz = execute_shell_command_and_return_stdout('timedatectl | grep "Time zone"').split("Time zone:")[1].split()[0]
    if tz == expected_tz:
        print("Timezone is OK")
    else:
        sys.stderr.write("[ERROR] Timezone is WRONG. Expected: {}. Current: {}.\n".format(expected_tz, tz))


def check_ntpd():
    """
    Checks that the ntpd.service is active and running.
    """
    try:
        ntpd_status = execute_shell_command_and_return_stdout('systemctl status ntpd | grep "Active:"').split("Active:")[1].strip()

        # TODO: Use the value of the config file, or we always want to check that is active and running?
        if ntpd_status.startswith("active (running)"):
            print("ntpd is active and running")
        else:
            sys.stderr.write("[ERROR] ntpd is not active and running. Current ntpd status: {}.\n".format(ntpd_status))
    except IndexError:
        # An IndexError can be raised by 2 reasons:
        # * The ntpd.service could not be found by the systemctl status command
        # * The grep command didn't find "Active:"
        # This means that ntpd is not configured correctly
        sys.stderr.write("[ERROR] ntpd.service could not be found.\n")


# TODO: In which machine is this method executed? bastion?
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
                print("The metrics endpoint {} is running.".format(metrics_endpoint_with_fqdn))
            else:
                sys.stderr.write("[ERROR] The metrics endpoint {} response wasn't 200 OK. HTTP status code: {}.\n"
                                 .format(metrics_endpoint_with_fqdn, response_code))

        except urllib.error.URLError as e:
            # If the endpoint is not running, an exception is raised
            sys.stderr.write("[ERROR] The metrics endpoint {} is not running. Reason: {}.\n".format(metrics_endpoint_with_fqdn, e.reason))


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

    # TODO: default="none" or default=None or without default?
    parser.add_argument('-c', '--configfile', help='Configuration file path (.json).', type=str, default=None, required=True)
    parser.add_argument('-t', '--type', help='Machine type (i.e. kafka).', type=str, default=None, required=True)
    parser.add_argument('-d', '--ec2-dummy', help='Path to the file that contains the simulation of the ec2-metadata command stdout.'
                                                  'If present, the ec2-metadata stdout will be simulated, using the file passed to this option',
                        type=str, default=None, required=False)

    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO)
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args, args.verbose)
