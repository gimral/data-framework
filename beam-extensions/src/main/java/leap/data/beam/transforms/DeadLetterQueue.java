package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroSerializer;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

public class DeadLetterQueue {

    public static <T> DeadLetterFn<T> of(String topic){
        return new AutoValue_DeadLetterQueue_DeadLetterFn.Builder<T>()
                .setTopic(topic)
                //.setBootstrapServers(boot)
                .build();
    }

    @AutoValue
    public abstract static class DeadLetterFn<T> extends PTransform<PCollection<T>, PDone> {

        abstract String getTopic();

        abstract String getBootstrapServers();

        abstract Map<String, Object> getProducerConfig();

        @Nullable
        abstract SerializableFunction<Map<String, Object>, Producer<Void,T>> getProducerFactoryFn();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setTopic(String topic);

            abstract Builder<T> setBootstrapServers(String bootstrapServers);

            abstract Builder<T> setProducerConfig(Map<String, Object> producerConfig);

            abstract Builder<T> setProducerFactoryFn(
                    SerializableFunction<Map<String, Object>, Producer<Void,T>> fn);

            abstract DeadLetterFn<T> build();
        }

        public DeadLetterFn<T> withBootstrapServers(String bootstrapServers) {
            return toBuilder().setBootstrapServers(bootstrapServers).build();
        }

//        public DeadLetterFn<T> withTopic(String topic) {
//            return toBuilder().setTopic(topic).build();
//        }

        public DeadLetterFn<T> updateProducerProperties(Map<String, Object> configUpdates) {
            Map<String, Object> config =
                    updateKafkaProperties(getProducerConfig(), IGNORED_PRODUCER_PROPERTIES, configUpdates);
            return toBuilder().setProducerConfig(config).build();
        }

        @Override
        public PDone expand(PCollection<T> input) {
            //TODO: Implement Dead Letter Logic
            return input.apply(ParDo.of(new DoFn<T,Object>() {
                @ProcessElement
                public void processElement(@Element T element, OutputReceiver r) {
                    r.output((Object) element);
                }
            })).apply(DeadLetterFn.class.getSimpleName(),
                    KafkaIO.<Void, Object>write()
                            .withBootstrapServers(getBootstrapServers())
                            .withTopic(getTopic())
                            .updateProducerProperties(getProducerConfig())
                            .withValueSerializer(LeapKafkaAvroSerializer.class)
                            .values()
            );
        }

        private static final Map<String, String> IGNORED_PRODUCER_PROPERTIES =
                ImmutableMap.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Use withKeySerializer instead",
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "Use withValueSerializer instead");

        private static Map<String, Object> updateKafkaProperties(
                Map<String, Object> currentConfig,
                Map<String, String> ignoredProperties,
                Map<String, Object> updates) {

            for (String key : updates.keySet()) {
                checkArgument(
                        !ignoredProperties.containsKey(key),
                        "No need to configure '%s'. %s",
                        key,
                        ignoredProperties.get(key));
            }

            Map<String, Object> config = new HashMap<>(currentConfig);
            config.putAll(updates);

            return config;
        }
    }
}
