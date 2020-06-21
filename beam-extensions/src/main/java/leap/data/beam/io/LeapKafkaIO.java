package leap.data.beam.io;

import com.google.auto.value.AutoValue;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.serializer.KafkaGenericRecordDeserializer;
import leap.data.beam.serializer.LeapAvroGenericCoder;
import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static leap.data.beam.configuration.KafkaPipelineOptions.getKafkaProperties;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class LeapKafkaIO {
    public static LeapGenericRecordRead readGeneric(){
        return new AutoValue_LeapKafkaIO_LeapGenericRecordRead.Builder()
                .setTopics(new ArrayList<>())
                .build();
    }

    public static <V> LeapSpecificRecordRead<V> readSpecific(){
        return new AutoValue_LeapKafkaIO_LeapSpecificRecordRead.Builder<V>()
                .setTopics(new ArrayList<>())
                .build();
    }

    @AutoValue
    public abstract static class LeapGenericRecordRead extends PTransform<PBegin,PCollection<KafkaRecord<Long, GenericRecord>>> {
        abstract List<String> getTopics();
        @Nullable
        abstract String getSchemaName();
        @Nullable
        abstract String getReaderSchema();
        @Nullable
        abstract Long getMaxNumRecords();
        @Nullable
        abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> getConsumerFactoryFn();
        abstract LeapKafkaIO.LeapGenericRecordRead.Builder toBuilder();

        @Override
        public PCollection<KafkaRecord<Long, GenericRecord>> expand(PBegin input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            Map<String,Object> kafkaProperties = getKafkaProperties(options);
            if(getReaderSchema() != null){
                kafkaProperties.put("schema.reader",getReaderSchema());
            }
            PCollection<KafkaRecord<Long, GenericRecord>> stream = null;
            try {
                KafkaIO.Read<Long, GenericRecord> reader = KafkaIO.<Long, GenericRecord>read()
                        .withBootstrapServers(options.getKafkaBootstrapServers())
                        .withTopics(getTopics())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withConsumerConfigUpdates(kafkaProperties);
                if(getReaderSchema() != null){
                    reader = reader.withValueDeserializerAndCoder(KafkaGenericRecordDeserializer.class,
                            LeapAvroGenericCoder.of(getReaderSchema()));
                }
                else{
                    reader = reader.withValueDeserializerAndCoder(KafkaGenericRecordDeserializer.class,
                            LeapAvroGenericCoder.of(getSchemaName(),kafkaProperties));
                }
                if(getMaxNumRecords() != null){
                    reader = reader.withMaxNumRecords(getMaxNumRecords());
                }
                if(getConsumerFactoryFn() != null){
                    reader = reader.withConsumerFactoryFn(getConsumerFactoryFn());
                }
                stream = input.apply("Read Kafka Stream " + getTopics().toString(), reader);
                //TODO: Handle Exceptions
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }

            return stream;
        }


        @AutoValue.Builder
        abstract static class Builder{
            abstract LeapKafkaIO.LeapGenericRecordRead.Builder setTopics(List<String> topics);
            abstract LeapKafkaIO.LeapGenericRecordRead.Builder setSchemaName(String schemaName);
            abstract LeapKafkaIO.LeapGenericRecordRead.Builder setMaxNumRecords(Long maxNumRecords);
            abstract LeapKafkaIO.LeapGenericRecordRead.Builder setReaderSchema(String readerSchema);
            abstract LeapKafkaIO.LeapGenericRecordRead.Builder setConsumerFactoryFn(
                    SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);
            abstract LeapKafkaIO.LeapGenericRecordRead build();
        }

        /**
         * Sets the topic to read from.
         */
        public LeapKafkaIO.LeapGenericRecordRead withTopic(String topic) {
            return withTopics(ImmutableList.of(topic));
        }

        /**
         * Sets a list of topics to read from. All the partitions from each of the topics are read.
         */
        public LeapKafkaIO.LeapGenericRecordRead withTopics(List<String> topics) {
            return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
        }
        /**
         * Sets the schema name to use. If this is not set schema name will be inferred from the record
         */
        public LeapKafkaIO.LeapGenericRecordRead withSchemaName(String schemaName) {
            return toBuilder().setSchemaName(schemaName).build();
        }


        /**
         * Sets a reader schema to be used by the deserializer.
         * Reader schema is used to deserialize only a subset of the fields.
         */
        public LeapKafkaIO.LeapGenericRecordRead withReaderSchema(Schema readerSchema) {
            return toBuilder().setReaderSchema(readerSchema.toString()).build();
        }

        /**
         * A factory to create Kafka {@link Consumer} from consumer configuration. This is useful for
         * supporting another version of Kafka consumer. Default is {@link KafkaConsumer}.
         */
        public LeapKafkaIO.LeapGenericRecordRead withConsumerFactoryFn(
                SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
            return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
        }

        public LeapKafkaIO.LeapGenericRecordRead withMaxNumRecords(Long maxNumRecords) {
            return toBuilder().setMaxNumRecords(maxNumRecords).build();
        }

    }

    @SuppressWarnings("unchecked")
    @AutoValue
    public abstract static class LeapSpecificRecordRead<V> extends PTransform<PBegin,PCollection<KafkaRecord<Long, V>>> {
        abstract List<String> getTopics();
        abstract String getReaderSchema();
        abstract Class<V> getValueCoderClass();
        abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> toBuilder();

        @Override
        public PCollection<KafkaRecord<Long, V>> expand(PBegin input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");
            checkArgument(getValueCoderClass() != null,
                    "ValueCoder should be set using withValueCoder method for Specific Records");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            Map<String,Object> kafkaProperties = getKafkaProperties(options);
            if(getReaderSchema() != null){
                kafkaProperties.put("schema.reader",getReaderSchema());
            }

            //noinspection rawtypes
            return (PCollection<KafkaRecord<Long, V>>) input.apply(
                    "Read Kafka Stream " + getTopics().toString(),
                    KafkaIO.<Long,V>read()
                            .withBootstrapServers(options.getKafkaBootstrapServers())
                            .withTopics(getTopics())
                            .withKeyDeserializer(LongDeserializer.class)
                            .withValueDeserializerAndCoder((Class) LeapKafkaAvroDeserializer.class,
                                    AvroCoder.of(getValueCoderClass()))
                            .withConsumerConfigUpdates(getKafkaProperties(options))
            );
        }


        @AutoValue.Builder
        abstract static class Builder<V>{
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setTopics(List<String> topics);
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setReaderSchema(String readerSchema);
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setValueCoderClass(Class<V> valueCoder);
            abstract LeapKafkaIO.LeapSpecificRecordRead<V> build();
        }

        /**
         * Sets the topic to read from.
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withTopic(String topic) {
            return withTopics(ImmutableList.of(topic));
        }

        /**
         * Sets a list of topics to read from. All the partitions from each of the topics are read.
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withTopics(List<String> topics) {
            return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
        }

        /**
         * Sets a reader schema to be used by the deserializer.
         * Reader schema is used to deserialize only a subset of the fields.
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withReaderSchema(Schema readerSchema) {
            return toBuilder().setReaderSchema(readerSchema.toString()).build();
        }

        /**
         * Sets coder for the SpecificRecord
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withValueCoderClass(Class<V> valueCoder) {
            return toBuilder().setValueCoderClass(valueCoder).build();
        }


    }


}
