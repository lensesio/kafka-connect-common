[![Build Status](https://travis-ci.org/datamountaineer/kafka-connect-common.svg?branch=master)](https://travis-ci.org/datamountaineer/kafka-connect-common)
[<img src="https://img.shields.io/badge/latest%20release-v0.2.2-blue.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.datamountaineer%22%20AND%20a%3A%22kafka-connect-common%22)

Kafka Connect Common is in Maven, include it in your connector.

```bash
#maven
<dependency>
	<groupId>com.datamountaineer</groupId>
	<artifactId>kafka-connect-common</artifactId>
	<version>0.2.2</version>
</dependency>

#sbt
libraryDependencies += "com.datamountaineer" % "kafka-connect-common" % "0.2.2"

#gradle
'com.datamountaineer:kafka-connect-common:0.2.2'
```


# kafka-connect-common
Common components used across the datamountaineer kafka connect connectors.

##Packages

###Config

####SSLConfigConext
Contains class for SSL Context configuration for supplied trust and keystores.

###Offsets

The offset handler retrieves, from Kafka the stored offset map per source partition.

###Queues

Helper methods to drain LinkedBlockingQueues.

###Sink

Contains Writer and KeyBuilder classes.

###DbWriter

Defines the contract for inserting a new row for the connect sink record.

####KeyBuilder

* Builds the new record key for the given connect SinkRecord.
* Builds a new key from the payload fields specified.

###Schemas

####PayloadFields
Works out the fields and their mappings to be used when inserting a new row.

###ConvertUtil

Converts source and sink records to JSON and Avro and back.

###StructFieldsExtractor

Extracts fields from a SinkRecord Struct based on a specified set of provided columns.
