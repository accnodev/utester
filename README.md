# utester
Unit Testing for Infrastructure

## Links
 
 - [Confluent’s Python client for Apache Kafka](https://docs.confluent.io/current/clients/confluent-kafka-python/)
 - [Python client for Apache Kafka](https://kafka-python.readthedocs.io/en/master/apidoc/kafka.html)  
 

## Kafka

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


## Redis

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
python utRedis.py -ho 192.168.56.51 -p 6379 -ssl -p `cat /root/psa/.psa.shadow`
```

### Connect without SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379
```

### Hello test

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -ssl -ht -p `cat /root/psa/.psa.shadow`
```

### Get key with SSL

```python
python utRedis.py -ho 192.168.56.51 -p 6379 -ssl -gk msg:hello -p `cat /root/psa/.psa.shadow`
```

 