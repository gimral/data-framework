## Create topics

```
confluent local start

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic AggregateProspect

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic ProspectAggregated-DQL

kafka-topics --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic LateProspect

```