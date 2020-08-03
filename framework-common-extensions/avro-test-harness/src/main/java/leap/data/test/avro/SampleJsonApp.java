package leap.data.test.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class SampleJsonApp {
    public void produceAccountToKafka(Account account) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonAccount = mapper.writeValueAsString(account);
        String topicName = "account-created";
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(getKafkaProperties());
        ProducerRecord<String,String> record = new ProducerRecord<>(topicName, null, jsonAccount);
        producer.send(record);
    }

    private Map<String,Object> getKafkaProperties(){
        Map<String,Object> props = new HashMap<>();
        props.put("bootstrap.servers", System.getenv().get("bootStrapServers"));
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}
