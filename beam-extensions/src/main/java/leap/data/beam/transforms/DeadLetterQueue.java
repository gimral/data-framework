package leap.data.beam.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.io.LeapKafkaIO;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class DeadLetterQueue {

    @SuppressWarnings("unchecked")
    public static <K,V> DeadLetterFn<K,V> of(String topic){
        return new AutoValue_DeadLetterQueue_DeadLetterFn.Builder<K,V>()
                .setTopic(topic)
                .build();
    }

    @AutoValue
    public abstract static class DeadLetterFn<K,V> extends PTransform<PCollection<KV<K,V>>, PDone> {

        private static final ObjectMapper objectMapper = new ObjectMapper();

        abstract String getTopic();

        abstract SimpleFunction<KV<K,V>, DeadLetterHeader> getHeaderFn();

        abstract Builder<K,V> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<K,V> {
            abstract Builder<K,V> setTopic(String topic);
            abstract Builder<K,V> setHeaderFn(SimpleFunction<KV<K,V>, DeadLetterHeader> headerFn);
            abstract DeadLetterFn<K,V> build();
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

                    //TODO:Convert this to a implemented DoFn
            return input.apply("Convert to Process Record", ParDo.of(new DoFn<KV<K,V>, ProducerRecord<K,V>>() {
                    @ProcessElement
                    public void processElement(@Element KV<K,V> element,  ProcessContext c) {
                        DeadLetterHeader header = getHeaderFn().apply(element);
                        ProducerRecord<K,V> record = new ProducerRecord<>(getTopic(), element.getKey(), element.getValue());
                        try {
                            record.headers().add(new RecordHeader("dead_letter_header", objectMapper.writeValueAsBytes(header)));
                        } catch (JsonProcessingException e) {
                            //TODO:Log Error
                            e.printStackTrace();
                        }
                        c.output(record);
                    }
                }))
                    .apply("Write To Dead Letter Topic " + getTopic(),
                    LeapKafkaIO.<K,V>writeRecords(options)
                            .withTopic(getTopic())
            );
        }

        public DeadLetterFn<K, V> withHeaderFn(SimpleFunction<KV<K,V>, DeadLetterHeader> headerFn) {
            return toBuilder().setHeaderFn(headerFn).build();
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
