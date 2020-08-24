package leap.data.beam.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.io.LeapKafkaIO;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class DeadLetterTransform<K,V> extends PTransform<PCollection<KV<K,V>>, PDone> {

        public static <K,V> DeadLetterTransform<K,V> of(String topic){
            return new Builder<K,V>()
                    .setTopic(topic)
                    .build();
        }
        
        private final String topic;

        private final ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn;

        private final SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn;

        private DeadLetterTransform(
                String topic,
                @Nullable ProcessFunction<KV<K, V>, DeadLetterHeader> headerFn,
                @Nullable SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
            this.topic = topic;
            this.headerFn = headerFn;
            this.producerFactoryFn = producerFactoryFn;
        }

        @Override
        public PDone expand(PCollection<KV<K,V>> input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");
            checkArgument(
                    getHeaderFn() != null,
                    "HeaderFn should be defined.");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            KvCoder<K,V> inputCoder = (KvCoder<K,V>) input.getCoder();

            return input.apply("Convert to Process Record", ParDo.of(new DeadLetterFn()))
                    .setCoder(ProducerRecordCoder.of(inputCoder.getKeyCoder(),inputCoder.getValueCoder()))
                    .apply("Write To Dead Letter Topic " + getTopic(),
                            LeapKafkaIO.<K,V>writeRecords(options)
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
            private Builder(DeadLetterTransform<K, V> source) {
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
            
            DeadLetterTransform<K, V> build() {
                String missing = "";
                if (this.topic == null) {
                    missing += " topic";
                }
                if (!missing.isEmpty()) {
                    throw new IllegalStateException("Missing required properties:" + missing);
                }
                return new DeadLetterTransform<K, V>(
                        this.topic,
                        this.headerFn,
                        this.producerFactoryFn);
            }
        }

        

        public DeadLetterTransform<K, V> withHeaderFn(ProcessFunction<KV<K,V>, DeadLetterHeader> headerFn) {
            return toBuilder().setHeaderFn(headerFn).build();
        }

        public DeadLetterTransform<K, V> withHeaderFn(SerializableFunction<KV<K,V>, DeadLetterHeader> headerFn) {
            return toBuilder().setHeaderFn(headerFn).build();
        }

        public DeadLetterTransform<K, V> withProducerFactoryFn(
                SerializableFunction<Map<String, Object>, Producer<K,V>> producerFactoryFn) {
            return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
        }

        /**
         * Writes just the values to Kafka. This is useful for writing collections of values rather
         * than {@link KV}s.
         */
        public PTransform<PCollection<V>, PDone> values() {
            return new DeadLetterQueueValue<>(this);
        }

        private class DeadLetterFn extends DoFn<KV<K,V>, ProducerRecord<K,V>>{
            private final ObjectMapper objectMapper = new ObjectMapper();
            @ProcessElement
            public void processElement(@Element KV<K,V> element,  ProcessContext c) throws Exception {
                DeadLetterHeader header = new DeadLetterHeader();
                if (getHeaderFn() != null) {
                    header = getHeaderFn().apply(element);
                }
                ProducerRecord<K,V> record = new ProducerRecord<>(getTopic(), element.getKey(), element.getValue());
                try {
                    record.headers().add(new RecordHeader("dead_letter_header", objectMapper.writeValueAsBytes(header)));
                } catch (JsonProcessingException e) {
                    //TODO:Log Error
                    e.printStackTrace();
                }
                c.output(record);
            }
        }


    private static class DeadLetterQueueValue<K,V> extends PTransform<PCollection<V>, PDone> {
        private final DeadLetterTransform<K,V> kvWriteTransform;

        private DeadLetterQueueValue(DeadLetterTransform<K,V> kvWriteTransform) {
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

    private static class NullOnlyCoder<T> extends AtomicCoder<T> {
        @Override
        public void encode(T value, OutputStream outStream) {
            checkArgument(value == null, "Can only encode nulls");
            // Encode as no bytes.
        }

        @Override
        public T decode(InputStream inStream) {
            return null;
        }
    }

    @SuppressWarnings("unused")
    @DefaultSchema(JavaBeanSchema.class)
    public static class DeadLetterHeader{
        private String originSource;
        private Integer originPartition;
        private Long originOffset;
        private String originId;
        private String applicationName;
        private String errorCategory;
        private String errorReason;
        private String errorDescription;
        private Integer retryCount;
        private String expectedSchema;
        private String actualSchema;

        public String getOriginSource() {
            return originSource;
        }

        public void setOriginSource(String originSource) {
            this.originSource = originSource;
        }

        public Integer getOriginPartition() {
            return originPartition;
        }

        public void setOriginPartition(Integer originPartition) {
            this.originPartition = originPartition;
        }

        public Long getOriginOffset() {
            return originOffset;
        }

        public void setOriginOffset(Long originOffset) {
            this.originOffset = originOffset;
        }

        public String getOriginId() {
            return originId;
        }

        public void setOriginId(String originId) {
            this.originId = originId;
        }

        public String getApplicationName() {
            return applicationName;
        }

        public void setApplicationName(String applicationName) {
            this.applicationName = applicationName;
        }

        public String getErrorCategory() {
            return errorCategory;
        }

        public void setErrorCategory(String errorCategory) {
            this.errorCategory = errorCategory;
        }

        public String getErrorReason() {
            return errorReason;
        }

        public void setErrorReason(String errorReason) {
            this.errorReason = errorReason;
        }

        public String getErrorDescription() {
            return errorDescription;
        }

        public void setErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
        }

        public Integer getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(Integer retryCount) {
            this.retryCount = retryCount;
        }

        public String getExpectedSchema() {
            return expectedSchema;
        }

        public void setExpectedSchema(String expectedSchema) {
            this.expectedSchema = expectedSchema;
        }

        public String getActualSchema() {
            return actualSchema;
        }

        public void setActualSchema(String actualSchema) {
            this.actualSchema = actualSchema;
        }
    }

}
