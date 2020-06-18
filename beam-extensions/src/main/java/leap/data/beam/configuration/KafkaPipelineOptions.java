package leap.data.beam.configuration;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Map;

@SuppressWarnings("unused")
public interface KafkaPipelineOptions extends PipelineOptions {
    @Description("Kafka Bootstrap Server")
    String getKafkaBootstrapServers();
    void setKafkaBootstrapServers(String kafkaBootstrapServers);

    @Description("Input Topics")
    Map<String,String> getInputTopics();
    void setInputTopics(Map<String,String> inputTopics);

    @Description("Output Topics")
    Map<String,String> getOutputTopics();
    void setOutputTopics(Map<String,String> outputTopics);
}

