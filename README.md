# utester
Unit Testing for Infrastructure

## Links
 
 - []()  
 


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

### Describe topic
    
```python
python utKafka.py -b 192.168.56.51:9092 -d topic -cf utester
```

### Delete Topic
    
```python
python utKafka.py -b 192.168.56.51:9092 -dt
```