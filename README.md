# kafka-connect-common
Common components used across the datamountaineer kafka connect connectors.

##Packages

###Config

####SSLConfigConext
Contains class for SSL Context configuration for supplied trust and keystores.

###Offsets

The offset handler retrieves, from Kafka the stored offset map per source partition.

###Queues

Helper methods to drain LinkedBlockingQueues

###Sink

Contains Writer and KeyBuilder classes.

###DbWriter

Defines the contract for inserting a new row for the connect sink record

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