package leap.data.framework.extension.confluent.json;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.serialization.AvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

public class LeapJsonToAvroSerializer implements AvroSerializer<Object> {
    private final LeapAvroSerializer avroSerializer;
    private final SchemaRegistryClient schemaRegistry;


    public LeapJsonToAvroSerializer(LeapSerializerConfig config){
        avroSerializer = new LeapAvroSerializer(config);
        //TODO: Make Singleton
        schemaRegistry = new SchemaRegistryClientFactory().getSchemaRegistryClient(config);
    }

    @Override
    public byte[] serialize(String subject, Object object) throws SerializationException {
        return serializeWithLatestVersion(subject, object);
    }

    public byte[] serializeWithVersion(String subject, Object object, int version) throws SerializationException {
        try{
            SchemaMetadata schemaMetadata = schemaRegistry.getSchemaMetadata(subject, version);
            return serializeWithSchemaMetadata(object, schemaMetadata);
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving Avro schemaMetaData for Subject: " + subject + " Version: " + version, e);
        }
    }

    public byte[] serializeWithLatestVersion(String subject, Object object) throws SerializationException {
        try{
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            return serializeWithSchemaMetadata(object, schemaMetadata);
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving latest Avro schemaMetaData for Subject: " + subject, e);
        }
    }

    public byte[] serializeWithSchemaMetadata(Object object, SchemaMetadata schemaMetadata) throws SerializationException {
        Schema schema = (new Schema.Parser()).parse(schemaMetadata.getSchema());
        DatumReader<Object> reader = new JsonGenericDatumReader<>(schema);
        //DatumReader<Object> reader = new GenericDatumReader<>(schema);
        //TODO: Get fom factory
        //DecoderFactory.get().jsonDecoder(schema, SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED);
        try{
            Decoder decoder = new ExtendedJsonDecoder(schema,object.toString());
            Object datum = reader.read(null, decoder);
            return avroSerializer.serializeWithSchemaMetadata(datum, schemaMetadata);
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    public GenericRecord serializeToGeneric(String subject, Object object){
        try {
            //TODO: Cache schema and reader
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            Schema schema = (new Schema.Parser()).parse(schemaMetadata.getSchema());
            DatumReader<Object> reader = new JsonGenericDatumReader<>(schema);
            Decoder decoder = new ExtendedJsonDecoder(schema,object.toString());
            return (GenericRecord) reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving latest Avro schemaMetaData for Subject: " + subject, e);
        }
    }

    public String getSubjectName(String topic, boolean isKey, Object value, Schema schema) {
        return avroSerializer.getSubjectName(topic, isKey, value, schema);
    }

}
