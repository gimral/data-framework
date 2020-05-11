package leap.data.framework.extension.confluent.avro;

import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.json.LeapJsonToAvroSerializer;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonFallBackAvroDeserializerDecorator implements AvroDeserializer<Object> {
    private final Logger logger = LoggerFactory.getLogger(JsonFallBackAvroDeserializerDecorator.class);
    private final AvroDeserializer<Object> decoratedDeserializer;
    private final LeapJsonToAvroSerializer jsonToAvroSerializer;
    //TODO: Do we need to take care concurrency? How to decide which serializer to use in concurrent calls?
    private long currentSerializerErrorCount;
    private boolean isAvroDefaultSerializer;
    private long maxErrorsBeforeFullFallback;
    private final String fallBackSchemaName;
    private final int fallBackSchemaVersion;

    public JsonFallBackAvroDeserializerDecorator(AvroDeserializer<Object> decoratedDeserializer, LeapSerializerConfig config) {
        this.decoratedDeserializer = decoratedDeserializer;
        jsonToAvroSerializer = new LeapJsonToAvroSerializer(config);
        currentSerializerErrorCount = 0;
        isAvroDefaultSerializer = true;
        maxErrorsBeforeFullFallback = 5;
        if(config.getProps().containsKey("serializer.avro.fallback.json.maxFullFallBackTry")){
            maxErrorsBeforeFullFallback = (Integer) config.getProps().get("serializer.avro.fallback.json.maxFullFallBackTry");
        }
        fallBackSchemaName = (String) config.getProps().get("serializer.avro.fallback.json.schema.name");
        fallBackSchemaVersion = 0;//(Integer) config.getProps().get("serializer.avro.fallback.json.schema.version");
    }

    @Override
    public Object deserialize(byte[] payload) {
        if(currentSerializerErrorCount >= maxErrorsBeforeFullFallback){
            isAvroDefaultSerializer = !isAvroDefaultSerializer;
            currentSerializerErrorCount = 0;
        }
        if(isAvroDefaultSerializer()) {
            return deserializeFromAvro(fallBackSchemaName, payload);
        }
        else{
            return deserializeFromJson(fallBackSchemaName, payload);
        }
    }

    private Object deserializeFromJson(String subject, byte[] payload){
        try{
            return jsonToAvroSerializer.serializeToGeneric(subject, new String(payload));
        }
        catch (Exception ex){
            //If caught a RestClientException no need to try with avro as that will also fail
            if(!isAvroDefaultSerializer && !(ex.getCause() instanceof RestClientException)) {
                logger.warn("JsonDeserializer failed retrying deserialization using AvroDeserializer.",ex);
                currentSerializerErrorCount++;
                return deserializeFromAvro(subject, payload);
            }
            throw new SerializationException("Json Serialization Failed", ex);
        }
    }

    private Object deserializeFromAvro(String subject, byte[] payload){
        try {
            return decoratedDeserializer.deserialize(payload);
        } catch (SerializationException ex) {
            if (isAvroDefaultSerializer &&
                    (ex.getCause().getMessage().equals("Unknown magic byte!") || ex.getCause().getCause() instanceof IOException)) {
                logger.warn("AvroDeserializer failed retrying deserialization using JsonDeserializer.",ex);
                currentSerializerErrorCount++;
                return deserializeFromJson(subject, payload);
            }
            throw ex;
        }
    }

    @Override
    public Object deserialize(byte[] payload, Schema readerSchema) {
        return decoratedDeserializer.deserialize(payload,readerSchema);
    }

    public static boolean isEnabled(LeapSerializerConfig config){
        return config.getProps().containsKey("serializer.avro.fallback.json.schema.name");
    }


    public boolean isAvroDefaultSerializer() {
        return isAvroDefaultSerializer;
    }

    public long getCurrentSerializerErrorCount() {
        return currentSerializerErrorCount;
    }
}
