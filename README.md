# kinesis_demo
A couple of sample apps, rewritten from this [aws labs repo](https://github.com/awslabs/kinesis-poster-worker).
A producer creates records in the following formats and puts them into the stream as JSON.
```
record = {'name': 'Arthur',
          'message': 'Hello world'}
```
The consumers taking this JSON data.

## Running the app
You will nee to have your AWS credentials in your environment to run this app.
```
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXX
```

### AWS Demo App
The _aws\_demo\_app_ is essentially a clone from the aws labs kinesis-poster-app. The readme for that repo can be found [here](https://github.com/awslabs/kinesis-poster-worker).

### My Messages App
The _my\_messages_ app is extremely similar to the AWS Demo App. The main difference is that there are two slightly different consumers in the My Messages app.
The `KinesisReadConsumer` formats and logs the records as they come in. The `KinesisDataConsumer` aggregates the number of each "message" and the name of the "author".

#### Starting the stream
This is the same command as the starting the producer. You will have to run it once before actually starting the consumer.
```
python my_messages/poster.py kinesis-demo
```
#### Starting the Producer
The default work time for the consumers is greater than that of the producers, so that the consumers can be started before the producers.
```
python my_messages/poster.py kinesis-demo
``` 
#### Starting the Read Consumer
```
python my_messages/read_consumer.py kinesis-demo
```
#### Starting the Data Consumer
```
python my_messages/data_consumer.py kinesis-demo
```


