# Kafka to Avro Writer
Kafka to Avro Writer based on Apache Beam. It reads data from multiple kafka topics and stores it on google cloud storage in Avro format.   
Natively Apache beam requires to provide Avro schema to work with GenericRecords.  
This solution uses custom Beam coder to allow dynamic serialization and deserialization of Avro GenericRecords with the use of Confluent Schema Registry. 
Thanks to this adding more topics with different types of data is just a matter of configuration.
Default configuration creates files on hourly basis per event type (essentially per kafka topic).  
The following configuration operates with **Google Dataflow** runner, but this code can also be run with other runners for example: **Flink runner**.  
If it is run with a different runner it can write files to different storage than google cloud storage since generic beam AvroIO class has been used here.

## Deploy streaming job to Google Dataflow
### Prerequisites
 * You need to have correct Dataflow permissions to deploy a job
 * Install google cloud sdk locally. Download [here](https://cloud.google.com/sdk/)
 * Build fat jar: 
 ```
 ./gradlew clean build
 ``` 
 
### Deploy
Replace the following parameters with your own:
* project - google cloud project
* schemaRegistryUrl - comma separated list of schema registry hosts with ports
* bootstrapServers - comma separated list of kafka bootstrap servers
* inputTopics - comma separated list of kafka topics that you want to read from
* network - Dataflow configuration of network: this is an optional parameter which you can omit in case you want to use google cloud platform defaults
* subnetwork - Dataflow configuration of subnetwork: this is an optional parameter which you can omit in case you want to use google cloud platform defaults  

**Launch the following command:**  
```
java -jar build/libs/kafka-to-avro-writer-*.jar --runner=DataflowRunner \
                   --project=gcp_project \
                   --jobName=kafka-to-avro-writer \
                   --consumerGroupId=kafka-to-avro-writer \
                   --schemaRegistryUrl=http://schema.registry.host1:8081,http://schema.registry.host2:8081 \
                   --bootstrapServers=kafka.host:9092 \
                   --offsetReset=earliest \
                   --numberOfShards=1 \
                   --basePath=gs://gcp_bucket/mds/facts \
                   --tempLocation=gs://gcp_bucket/temp/avro-writer \
                   --stagingLocation=gs://gcp_bucket/stream/staging/avro-writer \
                   --windowInMinutes=60 \
                   --zone=europe-west1-b \
                   --network=gcp_network \
                   --subnetwork=gcp_subnetwork \
                   --inputTopics='kafka_topic1,kafka_topic2' 
```
                   
**Other configuration parameters:**
* runner - runner for apache beam
* windowInMinutes - defines how often you want to flush windowed data into files (default 60 minutes)
* jobName - Dataflow job name
* consumerGroupId - kafka consumer group id
* offsetReset - kafka offset reset property
* numberOfShards - defines how many files per window duration will be created for one type of data (one topic)
* tempLocation - temp directory for Dataflow job on google cloud storage
* stagingLocation - staging directory for Dataflow job on google cloud storage 

## Read avro files
There are two ways to read our generated Avro files.  
First one is to keep working with GenericRecords in our code. It's ok but it might not be preferable for us to access our data by providing field names as strings.  

Second option is to work with generated SpecificRecords that we add to our classpath. This definitely makes development easier, but you need to be aware of Avro bug that is still present in the moment of writing this README for version **1.8.2**.
Link to Avro jira issue is [here](https://issues.apache.org/jira/browse/AVRO-1811)   
This bug is about conversion of GenericRecords to SpecificRecords where String and UTF8 types collide.
There are two ways to go around this until it's fixed:
* You can add ``` "avro.java.string": "String"``` to Avro schema for each String type field so that the registered Avro schema in Schema Registry will be the same as the one in avro java generated classes. Thanks to this our GenericRecords String types will be saved as String instead of UTF8
* If first option is impossible, because you might be already working with existing schemas or you don't want the overhead of adding this extra property to each String field the only solution right now is to patch org.apache.avro.generic.GenericData class.
I added the patched version to test sources as an example. It just returns String instead of UTF8 type during deepCopy conversions. You can find it [here](src/test/java/org/apache/avro/generic/GenericData.java)          

# License

    Copyright (C) 2016 Azimo

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.         
