package leap.data.beam.transforms.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import leap.data.beam.coder.NullOnlyCoder;
import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.io.LeapKafkaIO;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class DeadLetterWriteTransform<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterWriteTransform.class);

    public static <K, V> DeadLetterWriteTransform<K, V> to(String topic) {
        return new Builder<K, V>()
                .setTopic(topic)
                .build();
    }

    private final String topic;

    private final ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn;

    private final SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn;

    private DeadLetterWriteTransform(
            String topic,
            @Nullable ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn,
            @Nullable SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
        this.topic = topic;
        this.headerFn = headerFn;
        this.producerFactoryFn = producerFactoryFn;
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
        checkArgument(
                input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                "PipelineOptions should implement KafkaPipelineOptions");
        checkArgument(
                getHeaderFn() != null,
                "HeaderFn should be defined.");

        KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
        KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();

        return input.apply("Convert to Process Record", ParDo.of(new DeadLetterWriteFn()))
                .setCoder(ProducerRecordCoder.of(inputCoder.getKeyCoder(), inputCoder.getValueCoder()))
                .apply("Write To Dead Letter Topic " + getTopic(),
                        LeapKafkaIO.<K, V>writeRecords(options)
                                .withTopic(getTopic())
                                .withProducerFactoryFn(getProducerFactoryFn())
                );
    }

    String getTopic() {
        return topic;
    }

    @Nullable
    ProcessFunction<KV<K, V>, DeadLetterHeader> getHeaderFn() {
        return headerFn;
    }

    @Nullable
    SerializableFunction<Map<String, Object>, Producer<K, V>> getProducerFactoryFn() {
        return producerFactoryFn;
    }

    Builder<K, V> toBuilder() {
        return new Builder<>(this);
    }

    static final class Builder<K, V> {

        private String topic;
        private ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn;
        private SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn;

        Builder() {
        }

        private Builder(DeadLetterWriteTransform<K, V> source) {
            this.topic = source.getTopic();
            this.headerFn = source.getHeaderFn();
            this.producerFactoryFn = source.getProducerFactoryFn();
        }

        Builder<K, V> setTopic(String topic) {
            if (topic == null) {
                throw new NullPointerException("Null topic");
            }
            this.topic = topic;
            return this;
        }

        Builder<K, V> setHeaderFn(ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn) {
            this.headerFn = headerFn;
            return this;
        }

        Builder<K, V> setProducerFactoryFn(SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
            this.producerFactoryFn = producerFactoryFn;
            return this;
        }

        DeadLetterWriteTransform<K, V> build() {
            String missing = "";
            if (this.topic == null) {
                missing += " topic";
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new DeadLetterWriteTransform<K, V>(
                    this.topic,
                    this.headerFn,
                    this.producerFactoryFn);
        }
    }


    public DeadLetterWriteTransform<K, V> withHeaderFn(ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn) {
        return toBuilder().setHeaderFn(headerFn).build();
    }

    public DeadLetterWriteTransform<K, V> withHeaderFn(SerializableFunction<KV<K, V>, DeadLetterHeader> headerFn) {
        return toBuilder().setHeaderFn(headerFn).build();
    }

    public DeadLetterWriteTransform<K, V> withProducerFactoryFn(
            SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
        return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
    }

    /**
     * Writes just the values to Kafka. This is useful for writing collections of values rather
     * than {@link KV}s.
     */
    public PTransform<PCollection<V>, PDone> values() {
        return new DeadLetterQueueValue<>(this);
    }

    private class DeadLetterWriteFn extends DoFn<KV<K, V>, ProducerRecord<K, V>> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private final Counter deadLetterCounter;

        private DeadLetterWriteFn() {
            deadLetterCounter = Metrics.counter(this.getClass(), "deadLetteredMessages");
        }

        @ProcessElement
        public void processElement(@Element KV<K, V> element, ProcessContext c) throws Exception {
            DeadLetterHeader header = new DeadLetterHeader();
            if (getHeaderFn() != null) {
                header = getHeaderFn().apply(element);
            }
            ProducerRecord<K, V> record = new ProducerRecord<>(getTopic(), element.getKey(), element.getValue());
            try {
                record.headers().add(new RecordHeader("dead_letter_header", objectMapper.writeValueAsBytes(header)));
            } catch (JsonProcessingException e) {
                logger.error("Error processing header to json", e);
            }
            deadLetterCounter.inc();
            c.output(record);

        }
    }

    private static class DeadLetterQueueValue<K, V> extends PTransform<PCollection<V>, PDone> {
        private final DeadLetterWriteTransform<K, V> kvWriteTransform;

        private DeadLetterQueueValue(DeadLetterWriteTransform<K, V> kvWriteTransform) {
            this.kvWriteTransform = kvWriteTransform;
        }

        @Override
        public PDone expand(PCollection<V> input) {
            return input
                    .apply(
                            "Kafka values with default key",
                            MapElements.via(
                                    new SimpleFunction<V, KV<K, V>>() {
                                        @Override
                                        public KV<K, V> apply(V element) {
                                            return KV.of(null, element);
                                        }
                                    }))
                    .setCoder(KvCoder.of(new NullOnlyCoder<>(), input.getCoder()))
                    .apply(kvWriteTransform);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            kvWriteTransform.populateDisplayData(builder);
        }
    }



}
