package leap.data.beam.io;

import com.google.auto.value.AutoValue;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.serializer.KafkaGenericRecordDeserializer;
import leap.data.beam.serializer.KafkaRecordSerializer;
import leap.data.beam.serializer.LeapAvroGenericCoder;
import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static leap.data.beam.configuration.KafkaPipelineOptions.getKafkaProperties;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("InstantiatingObjectToGetClassObject")
public class LeapKafkaIO {

    public static <K> KafkaIO.Read<K, GenericRecord> readGeneric(KafkaPipelineOptions options,
                                                                String schemaName) throws Exception{
        return readGeneric(options, schemaName, null);
    }

    public static <K> KafkaIO.Read<K, GenericRecord> readGeneric(KafkaPipelineOptions options,
                                                                Schema readerSchema) throws Exception{
        return readGeneric(options, null, readerSchema);
    }

    private static <K> KafkaIO.Read<K, GenericRecord> readGeneric(KafkaPipelineOptions options,
                                                                String schemaName, Schema readerSchema) throws Exception {
        Map<String,Object> kafkaProperties = getKafkaProperties(options);
        if(readerSchema != null){
            kafkaProperties.put("schema.reader",readerSchema.toString());
        }
        try {
            KafkaIO.Read<K, GenericRecord> reader = KafkaIO.<K, GenericRecord>read()
                    .withBootstrapServers(options.getKafkaBootstrapServers())
                    .withConsumerConfigUpdates(kafkaProperties);
            if(readerSchema != null){
                reader = reader.withValueDeserializerAndCoder(KafkaGenericRecordDeserializer.class,
                        LeapAvroGenericCoder.of(readerSchema.toString()));
            }
            else{
                reader = reader.withValueDeserializerAndCoder(KafkaGenericRecordDeserializer.class,
                        LeapAvroGenericCoder.of(schemaName,kafkaProperties));
            }
            return reader;
            //TODO: Handle Exceptions
        } catch (IOException | RestClientException e) {
            e.printStackTrace();
            throw new Exception(e);
        }

    }

    public static LeapGenericRecordRead readGenericDefault(){
        return new AutoValue_LeapKafkaIO_LeapGenericRecordRead.Builder()
                .setTopics(new ArrayList<>())
                .build();
    }

    public static <V extends SpecificRecordBase> LeapSpecificRecordRead<V> readSpecific(){
        return new AutoValue_LeapKafkaIO_LeapSpecificRecordRead.Builder<V>()
                .setTopics(new ArrayList<>())
                .build();
    }

    public static <V> LeapRecordWrite<V> writeDefault(){
        return new AutoValue_LeapKafkaIO_LeapRecordWrite.Builder<V>()
                .setTopic("")
                .build();
    }

    public static <K,V> KafkaIO.Write<K, V> write(KafkaPipelineOptions options){
        Map<String,Object> kafkaProperties = getKafkaProperties(options);

        @SuppressWarnings("unchecked")
        KafkaIO.Write<K,V> writer = KafkaIO.<K,V>write()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withProducerConfigUpdates(kafkaProperties)
                .withValueSerializer((Class<? extends Serializer<V>>) new KafkaRecordSerializer<>().getClass());
        return writer;
    }

    public static <K,V> KafkaIO.WriteRecords<K, V> writeRecords(KafkaPipelineOptions options){
        Map<String,Object> kafkaProperties = getKafkaProperties(options);

        @SuppressWarnings("unchecked")
        KafkaIO.WriteRecords<K,V> writer = KafkaIO.<K,V>writeRecords()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withProducerConfigUpdates(kafkaProperties)
                .withValueSerializer((Class<? extends Serializer<V>>) new KafkaRecordSerializer<>().getClass());
        return writer;
    }

    @AutoValue
    public abstract static class LeapGenericRecordRead extends PTransform<PBegin,PCollection<KafkaRecord<String, GenericRecord>>> {
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
        public PCollection<KafkaRecord<String, GenericRecord>> expand(PBegin input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            Map<String,Object> kafkaProperties = getKafkaProperties(options);
            if(getReaderSchema() != null){
                kafkaProperties.put("schema.reader",getReaderSchema());
            }
            PCollection<KafkaRecord<String, GenericRecord>> stream = null;
            try {
                KafkaIO.Read<String, GenericRecord> reader = KafkaIO.<String, GenericRecord>read()
                        .withBootstrapServers(options.getKafkaBootstrapServers())
                        .withTopics(getTopics())
                        .withKeyDeserializer(StringDeserializer.class)
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
                stream = input.apply("Read Kafka " + getTopics().toString(), reader);
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
    public abstract static class LeapSpecificRecordRead<V extends SpecificRecordBase> extends PTransform<PBegin,PCollection<KafkaRecord<Long, V>>> {
        abstract List<String> getTopics();
        @Nullable
        abstract Class<V> getValueCoderClass();
        @Nullable
        abstract Long getMaxNumRecords();
        @Nullable
        abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> getConsumerFactoryFn();

        abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> toBuilder();

        @SuppressWarnings("rawtypes")
        @Override
        public PCollection<KafkaRecord<Long, V>> expand(PBegin input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");
            checkArgument(getValueCoderClass() != null,
                    "ValueCoder should be set using withValueCoder method for Specific Records");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            Map<String,Object> kafkaProperties = getKafkaProperties(options);
            kafkaProperties.put("specific.avro.reader", "true");

            KafkaIO.Read<Long, V> reader = KafkaIO.<Long, V>read()
                        .withBootstrapServers(options.getKafkaBootstrapServers())
                        .withTopics(getTopics())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializerAndCoder((Class) LeapKafkaAvroDeserializer.class,
                                AvroCoder.of(getValueCoderClass()))
                        .withConsumerConfigUpdates(kafkaProperties);

            if(getMaxNumRecords() != null){
                reader = reader.withMaxNumRecords(getMaxNumRecords());
            }
            if(getConsumerFactoryFn() != null){
                reader = reader.withConsumerFactoryFn(getConsumerFactoryFn());
            }
            return input.apply("Read Kafka " + getTopics().toString(), reader);
        }


        @AutoValue.Builder
        abstract static class Builder<V extends SpecificRecordBase>{
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setTopics(List<String> topics);
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setValueCoderClass(Class<V> valueCoder);
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setMaxNumRecords(Long maxNumRecords);
            abstract LeapKafkaIO.LeapSpecificRecordRead.Builder<V> setConsumerFactoryFn(
                    SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);
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
         * Sets coder for the SpecificRecord
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withValueCoderClass(Class<V> valueCoder) {
            return toBuilder().setValueCoderClass(valueCoder).build();
        }

        /**
         * A factory to create Kafka {@link Consumer} from consumer configuration. This is useful for
         * supporting another version of Kafka consumer. Default is {@link KafkaConsumer}.
         */
        public LeapKafkaIO.LeapSpecificRecordRead<V> withConsumerFactoryFn(
                SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
            return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
        }

        public LeapKafkaIO.LeapSpecificRecordRead<V> withMaxNumRecords(Long maxNumRecords) {
            return toBuilder().setMaxNumRecords(maxNumRecords).build();
        }

    }

    @SuppressWarnings("InstantiatingObjectToGetClassObject")
    @AutoValue
    public abstract static class LeapRecordWrite<V> extends PTransform<PCollection<KV<String, V>>, PDone> {
        abstract String getTopic();
        @Nullable
        abstract SerializableFunction<Map<String, Object>, Producer<String, V>> getProducerFactoryFn();
        abstract LeapKafkaIO.LeapRecordWrite.Builder<V> toBuilder();

        @Override
        public PDone expand(PCollection<KV<String, V>> input) {
            checkArgument(
                    input.getPipeline().getOptions() instanceof KafkaPipelineOptions,
                    "PipelineOptions should implement KafkaPipelineOptions");

            KafkaPipelineOptions options = (KafkaPipelineOptions) input.getPipeline().getOptions();
            Map<String,Object> kafkaProperties = getKafkaProperties(options);

            @SuppressWarnings("unchecked")
            KafkaIO.Write<String,V> writer = KafkaIO.<String,V>write()
                    .withBootstrapServers(options.getKafkaBootstrapServers())
                    .withTopic(getTopic())
                    .withProducerConfigUpdates(kafkaProperties)
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer((Class<? extends Serializer<V>>) new KafkaRecordSerializer<>().getClass());

            if(getProducerFactoryFn() != null){
                writer = writer.withProducerFactoryFn(getProducerFactoryFn());
            }

            return input.apply(writer);
        }

        @AutoValue.Builder
        abstract static class Builder<V>{
            abstract LeapKafkaIO.LeapRecordWrite.Builder<V> setTopic(String topic);
            abstract LeapKafkaIO.LeapRecordWrite.Builder<V> setProducerFactoryFn(
                    SerializableFunction<Map<String, Object>, Producer<String, V>> fn);
            abstract LeapKafkaIO.LeapRecordWrite<V> build();
        }

        /**
         * Sets the topic to read from.
         */
        public LeapKafkaIO.LeapRecordWrite<V> withTopic(String topic) {
            return toBuilder().setTopic(topic).build();
        }

        /**
         * Sets a custom function to create Kafka producer. Primarily used for tests. Default is {@link
         * KafkaProducer}
         */
        public LeapKafkaIO.LeapRecordWrite<V> withProducerFactoryFn(
                SerializableFunction<Map<String, Object>, Producer<String,V>> producerFactoryFn) {
            return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
        }

        /**
         * Writes just the values to Kafka. This is useful for writing collections of values rather
         * than {@link KV}s.
         */
        public PTransform<PCollection<V>, PDone> values() {
            return new LeapKafkaIO.KafkaValueWrite<>(this);
        }

    }

    /**
     * Same as {@code Write<K, V>} without a Key. Null is used for key as it is the convention is
     * Kafka when there is no key specified. Majority of Kafka writers don't specify a key.
     */
    private static class KafkaValueWrite<V> extends PTransform<PCollection<V>, PDone> {
        private final LeapKafkaIO.LeapRecordWrite<V> kvWriteTransform;

        private KafkaValueWrite(LeapKafkaIO.LeapRecordWrite<V> kvWriteTransform) {
            this.kvWriteTransform = kvWriteTransform;
        }

        @Override
        public PDone expand(PCollection<V> input) {
            return input
                    .apply(
                            "Kafka values with default key",
                            MapElements.via(
                                    new SimpleFunction<V, KV<String, V>>() {
                                        @Override
                                        public KV<String, V> apply(V element) {
                                            return KV.of(null, element);
                                        }
                                    }))
                    .setCoder(KvCoder.of(new LeapKafkaIO.NullOnlyCoder<>(), input.getCoder()))
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




}
