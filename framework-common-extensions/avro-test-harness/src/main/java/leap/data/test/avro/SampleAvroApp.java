package leap.data.test.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class SampleAvroApp {
    public void produceAccountToKafka(Account account) throws JsonProcessingException {
        String topicName = "account-created";
        KafkaProducer<String,Account> producer = new KafkaProducer<String, Account>(getKafkaProperties());
        ProducerRecord<String,Account> record = new ProducerRecord<>(topicName, null, account);
        producer.send(record);
    }

    private Map<String,Object> getKafkaProperties(){
        Map<String,Object> props = new HashMap<>();
        props.put("bootstrap.servers", System.getenv().get("bootStrapServers"));
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "leap.data.framework.extension.confluent.kafka.LeapKafkaAvroSerializer");
        props.put("schema.registry.url",System.getenv().get("schemaRegistryUrl"));
        return props;
    }

}
