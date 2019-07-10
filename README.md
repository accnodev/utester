# utester
Unit Testing for Infrastructure


## Links

 - [Confluent’s Python client for Apache Kafka](https://docs.confluent.io/current/clients/confluent-kafka-python/)
 - [Python client for Apache Kafka](https://kafka-python.readthedocs.io/en/master/apidoc/kafka.html)  

---

## Kafka

Unit Tester for __Kafka__ deployment.

```bash
python utKafka.py --help
```
 - Test Producer from command lines. This will autocreate a topic named 'utester'
 - Test Delete topic
 - Test Describe resource with filter

### Create lines

```python
python utKafka.py -b 192.168.56.51:9092 -pl
```

### Describe topic

```python
python utKafka.py -b 192.168.56.51:9092 -d topic -cf utester
```

### Delete Topic

```python
python utKafka.py -b 192.168.56.51:9092 -dt
```

---

## Redis

Unit Tester for __Redis__ deployment.

```bash
python utRedis.py --help
```

 - Connect using ssl.
 - conenct without ssl.
 - Hello test. Send Hello message to Redis and get it back.
 - Get by ky. Get message by key.

### Connect using SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl
```

### Connect without SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow`
```

### Hello test with SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -ht
```

### Get all keys

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -ak
```

### Flush all (delete all keys in all databases) with SSL
    
```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -fa
```
### Get key with SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -gk msg:hello
```

### Delete key (or keys, separated by spaces) with SSL
```python
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -dk key1
python utRedis.py -ho 192.168.56.51 -p 6379 -pw `cat /root/psa/.psa.shadow` -ssl -dk key1 key2 key3
```

## Hardware

Unit Tester of the __hardware__ of a machine.

```bash
python utHardware.py --help
```
Possible machine types:
 - bastion
 - kafka
 - striim
 - psql
 - emr
 - redis
 - psql2
 - dns: This type checks that the fqdns are resolved by the DNS. Must be executed from bastion.

If this file is NOT executed in an EC2 instance, the --ec2-dummy must be used, pointing to a file that simulates the ec2-metadata command.

### Check an EC2 kafka machine

```bash
python utHardware.py --configfile config/config.global.json --type kafka
```

### Check a kafka machine that is NOT an EC2 instance (for testing purposes, for example)
```bash
python utHardware.py --configfile config/config.global.json --type kafka --dummy config/ec2-metadata-dummy.global.txt
```

### Check that the fqdns of the rest of machines are resolved by the DNS, from the EC2 bastion machine
```bash
python utHardware.py --configfile config/config.global.json --type dns
```
