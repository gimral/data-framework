**Confluent Avro Extension**
-

This library enhances the functionality of the Confluent Avro Serializer library.

- Usage

Add dependency to the library

Maven
```xml
<dependency>
    <groupId>leap-data</groupId>
    <artifactId>confluent-avro-extension</artifactId>
    <version>0.1.0</version>
</dependency>
```

If using for Kafka consumers and producers set the configuration as:

Producer
```properties
value.serializer=leap.data.framework.extension.confluent.kafka.LeapKafkaAvroSerializer
```
Consumer
```properties
value.deserializer=leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer
```

**Features**
-
- Resiliency

Library enhances the SchemaRegistry client by implementing a retry mechanism with a fluent backoff design.

<table>
<tr>
<th>Config</th>
<th>Description</th>
<th>Default Value</th>
</tr>
<tr>
<td>serializer.registry.retry.maxRetries</td>
<td>Max number of retries to perform</td>
<td>10</td>
</tr>
<tr>
<td>serializer.registry.retry.initialBackoff</td>
<td>Initial backoff in milliseconds before retry</td>
<td>1000</td>
</tr>
<tr>
<td>serializer.registry.retry.maxBackoff</td>
<td>Maximum backoff in milliseconds</td>
<td>5 minutes</td>
</tr>
<tr>
<td>serializer.registry.retry.exponent</td>
<td>Exponent to use when calculating next backoff</td>
<td>2</td>
</tr>
<tr>
<td>serializer.registry.retry.maxCumulativeBackoff</td>
<td>Total backoff in milliseconds to reach before stopping retries</td>
<td>30 minutes</td>
</tr>
</table>

With default settings the retries will be performed as:

<table>
<tr>
<td>#</td><td>Backoff in Seconds</td>
</tr>
<tr><td>1</td><td>1</td></tr>
<tr><td>2</td><td>2</td></tr>
<tr><td>3</td><td>4</td></tr>
<tr><td>4</td><td>8</td></tr>
<tr><td>5</td><td>16</td></tr>
<tr><td>6</td><td>32</td></tr>
<tr><td>7</td><td>64</td></tr>
<tr><td>8</td><td>128</td></tr>
<tr><td>9</td><td>256</td></tr>
<tr><td>10</td><td>512</td></tr>
</table>

- Security

OAuth2 authentication support is implemented with refreshable tokens.
Library sets the OAuth2 authentication as the default. 

<table>
<tr><td>Config</td><td>Description</td></tr>
<tr><td>bearer.auth.host</td><td>Authentication host name</td></tr>
<tr><td>bearer.auth.clientId</td><td>Client Id</td></tr>
<tr><td>bearer.auth.clientSecret</td><td>Client Secret</td></tr>
</table>

- Json to Avro Serialization

Library implements a serializer that can convert Json messages into Avro format 
and a deserializer that can convert Avro payloads into Json messages.
Messages will be written to Kafka in Avro format while the application can continue working with the Json messages.

To use the JsonSerializers in Kafka set the configuration as:

Producer
```properties
value.serializer=leap.data.framework.extension.confluent.json.LeapJsonToAvroSerializer
```
Consumer
```properties
value.deserializer=leap.data.framework.extension.confluent.json.LeapJsonToAvroDeserializer
```

- Json Fallback for Avro Deserializer

When using Avro deserializer, it is possible to fallback to using JsonDeserializer when a deserialization exception oocurs.
This would be useful when the source has mixed messages(migration period) or wanting to process the old messages.

<table>
<tr>
<th>Config</th>
<th>Description</th>
<th>Default Value</th>
</tr>
<tr>
<td>serializer.avro.fallback.json.schema.name</td>
<td>Schema to use for Json deserialization</td>
<td></td>
</tr>
<tr>
<td>serializer.avro.fallback.json.maxFullFallBackTry</td>
<td>Number of tries before setting the Json as the default deserializer</td>
<td>5</td>
</tr>
</table>