[![Build Status](https://travis-ci.org/Landoop/kafka-connect-common.svg?branch=master)](https://travis-ci.org/Landoop/kafka-connect-common)
[<img src="https://img.shields.io/badge/latest%20release-v01.0.4-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datamountaineer%22%20AND%20a%3A%22kafka-connect-common%22)
Kafka Connect Common is in Maven, include it in your connector.


# Releases


| Version | Confluent Version |Kafka| Kcql Version |
| ------- | ----------------- |-----|--------------|
|1.0.4|4.0.0|1.0.0|2.5.1
|1.0.3|4.0.0|1.0.0|2.4
|1.0.2|4.0.0|1.0.0|2.4
|1.0.1|4.0.0|1.0.0|2.4
|1.0.2|4.0.0|1.0.0|2.4
|1.0.1|4.0.0|1.0.0|2.4
|1.0.0|4.0.0|1.0.0|2.4
|0.9.6.4|3.3.0|0.11.0.0|2.1
|0.9.6.3|3.3.0|0.11.0.0|2.1
|0.9.6.2|3.3.0|0.11.0.0|2.1
|0.9.6.1|3.3.0|0.11.0.0|2.1
|0.9.6|3.3.0|0.11.0.0|2.1
|0.9.5|3.3.0|0.11.0.0|2.1
|0.9.4|3.3.0|0.11.0.0|2.1
|0.9.3|3.3.0|0.11.0.0|2.1
|0.9.2|3.3.0|0.11.0.0|2.1
|0.9.1|3.3.0|0.11.0.0|2.1
|0.9.0|3.3.0|0.11.0.0|2.1
|0.8.3.2|3.3.0|0.11.0.0|2.1
|0.8.3.1|3.3.0|0.11.0.0|2.1
|0.8.3.0|3.3.0|0.11.0.0|2.1
|0.8.2.9|3.3.0|0.11.0.0|2.1
|0.8.2.8|3.3.0|0.11.0.0|2.1
|0.8.2.7|3.3.0|0.11.0.0|2.1
|0.8.2.6|3.3.0|0.11.0.0|2.1
|0.8.2.5|3.3.0|0.11.0.0|2.1
|0.8.2.4|3.3.0|0.11.0.0|2.1
|0.8.1.3|3.2.2|0.10.2.0|2.1
|0.8.0|3.2.1|0.10.2.0|2.1
|0.7.9|3.2.1|0.10.2.0|
|0.7.8|3.2.0|0.10.2.0|
|0.7.7|3.2.0|0.10.2.0|
|0.7.6|3.2.0|0.10.2.0|
|0.7.5|3.2.0|0.10.2.0|
|0.7.4|3.2.0|0.10.2.0|
|0.7.3|3.2.0|0.10.2.0|
|0.7.2|3.2.0|0.10.2.0|
|0.7.1|3.2.0|0.10.2.0|
|0.7.0|3.1.1|0.10.1.1|
|0.6.9|3.1.1|0.10.1.1|
|0.6.8|3.1.1|0.10.1.1|
|0.6.7|3.1.1|0.10.1.1|
|0.6.6|3.0.1|0.10.0.1|
|0.6.5|3.0.1|0.10.0.1|
|0.6.4|3.0.1|0.10.0.1|
|0.6.3|3.0.1|0.10.0.1|
|0.6.2|3.0.1|0.10.0.1|
|0.6.1|3.0.1|0.10.0.1|
|0.5|3.0.1|0.10.0.1|
|0.4.2|3.0.0|0.10.0.0|
|0.4.1|3.0.0|0.10.0.0|
|0.4|3.0.0|0.10.0.0|
|0.3.8|3.0.0||
|0.3.7|3.0.0||
|0.3.5|2.0.1||
|0.3.4|2.0.1||
|0.3.3|2.0.1||


```bash
#maven
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kafka-connect-common</artifactId>
	<version>1.0.4</version>
</dependency>

#sbt
libraryDependencies += "com.datamountaineer" % "kafka-connect-common" % "1.0.4"

#gradle
'com.datamountaineer:kafka-connect-common:1.0.4'
```

# kafka-connect-common
Common components used across the datamountaineer kafka connect connectors.

## Packages

### Config

#### SSLConfigConext
Contains class for SSL Context configuration for supplied trust and keystores.

### Offsets

The offset handler retrieves, from Kafka the stored offset map per source partition.

### Queues

Helper methods to drain LinkedBlockingQueues.

### Sink

Contains Writer and KeyBuilder classes.

### DbWriter

Defines the contract for inserting a new row for the connect sink record.

#### KeyBuilder

* Builds the new record key for the given connect SinkRecord.
* Builds a new key from the payload fields specified.

### Schemas

* RestService to integrate with the Schema Registry

#### PayloadFields
Works out the fields and their mappings to be used when inserting a new row.

### ConvertUtil

Converts source and sink records to JSON and Avro and back.

### StructFieldsExtractor

Extracts fields from a SinkRecord Struct based on a specified set of provided columns.
