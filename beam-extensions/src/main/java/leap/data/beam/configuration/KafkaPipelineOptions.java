package leap.data.beam.configuration;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public interface KafkaPipelineOptions extends LeapPipelineOptions {
    @Description("Kafka Bootstrap Servers")
    String getKafkaBootstrapServers();
    void setKafkaBootstrapServers(String kafkaBootstrapServers);

    @Description("Kafka Security Protocol")
    @Default.String("SASL_SSL")
    String getKafkaSecurityProtocol();
    void setKafkaSecurityProtocol(String kafkaSecurityProtocol);

    @Description("Kafka Sasl Mechanism")
    @Default.String("SCRAM-SHA-512")
    String getKafkaSaslMechanism();
    void setKafkaSaslMechanism(String kafkaSaslMechanism);

    @Description("Kafka Group Id")
    String getKafkaGroupId();
    void setKafkaGroupId(String kafkaGroupId);

    @Description("Kafka Auto Offset Reset")
    @Default.String("latest")
    String getKafkaAutoOffsetReset();
    void setKafkaAutoOffsetReset(String kafkaAutoOffsetReset);

    @Description("Kafka Ssl Trust Store Location")
    @Default.String("src/main/resources/kafka.client.truststore.jks")
    String getKafkaSslTrustStoreLocation();
    void setKafkaSslTrustStoreLocation(String kafkaSslTrustStoreLocation);

    @Description("Kafka Ssl Trust Store Password")
    String getKafkaSslTrustStorePassword();
    void setKafkaSslTrustStorePassword(String kafkaSslTrustStorePassword);

    @Description("Kafka Retries")
    @Default.String("2147483647")
    String getKafkaRetries();
    void setKafkaRetries(String kafkaRetries);

    @Description("Kafka Schema Registry URL")
    String getKafkaSchemaRegistryUrl();
    void setKafkaSchemaRegistryUrl(String kafkaSchemaRegistryUrl);

    @Description("Kafka Sasl User Name")
    String getKafkaSaslUserName();
    void setKafkaSaslUserName(String kafkaSaslUserName);

    @Description("Kafka Sasl Password")
    String getKafkaSaslPassword();
    void setKafkaSaslPassword(String kafkaSaslUserName);

    @Description("Input Topics")
    Map<String,String> getInputTopics();
    void setInputTopics(Map<String,String> inputTopics);

    @Description("Output Topics")
    Map<String,String> getOutputTopics();
    void setOutputTopics(Map<String,String> outputTopics);

    static Map<String,Object> getKafkaProperties(KafkaPipelineOptions options){
        Map<String,Object> props = new HashMap<>();
        props.put("security.protocol", options.getKafkaSecurityProtocol());
        if(!options.getKafkaSaslMechanism().equals("")){
            props.put("sasl.mechanism", options.getKafkaSaslMechanism());
            if(options.getKafkaSaslPassword() != null && options.getKafkaSaslPassword().equals(""))
                props.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
                        options.getKafkaSaslUserName() + "\" password=\"" + System.getenv("EDP_SECRET") + "\";");
            else if(options.getKafkaSaslPassword() !=  null)
                props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        options.getKafkaSaslUserName() + "\" password=\"" +
                        new String(Base64.getDecoder().decode(options.getKafkaSaslPassword())) + "\";");
        }
        props.put("group.id",options.getKafkaGroupId());
        props.put("auto.offset.reset",options.getKafkaAutoOffsetReset());
        if(options.getKafkaSslTrustStorePassword() !=  null) {
            props.put("ssl.endpoint.identification.algorithm", "");
            props.put("ssl.truststore.location", options.getKafkaSslTrustStoreLocation());
            props.put("ssl.truststore.password", new String(Base64.getDecoder().decode(options.getKafkaSslTrustStorePassword())));
        }
        props.put("retries", options.getKafkaRetries());
        props.put("schema.registry.url", options.getKafkaSchemaRegistryUrl());
        if(options.getKafkaGroupId() != null)
            props.put("enable.auto.commit", "true");
        return props;
    }
}

