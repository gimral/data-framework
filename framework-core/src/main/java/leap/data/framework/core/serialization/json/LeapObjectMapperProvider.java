package leap.data.framework.core.serialization.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import leap.framework.model.EncryptedField;
import leap.framework.serialization.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.math.BigDecimal;
import java.time.LocalDate;

public class LeapObjectMapperProvider {

    public static ObjectMapper getGenericRecordSensistiveObjectMapper(){
        final ObjectMapper sensitiveMapper = getSensitiveObjectMapper();
        final SimpleModule simpleModule = getDefaultSensitiveModule();
        simpleModule.addSerializer(GenericRecord.class,new GenericRecordSerializer());
        sensitiveMapper.registerModule(simpleModule);
        return sensitiveMapper;
    }

    public static ObjectMapper getSensitiveObjectMapper(){
        final ObjectMapper sensitiveMapper = getDefaultObjectMapper();
        final SimpleModule simpleModule = getDefaultSensitiveModule();
        sensitiveMapper.registerModule(simpleModule);
        return sensitiveMapper;
    }

    private static ObjectMapper getDefaultObjectMapper(){
        final ObjectMapper sensitiveMapper = new ObjectMapper();

        sensitiveMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        sensitiveMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        sensitiveMapper.registerModule(new JavaTimeModule());
        sensitiveMapper.registerModule(new JodaModule());
        sensitiveMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        sensitiveMapper.addMixIn(IndexedRecord.class, LeapObjectMapperProvider.AvroMixIn.class);

        sensitiveMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        sensitiveMapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean hasIgnoreMarker(AnnotatedMember m) {
                return _findAnnotation(m, IgnoreWhenLogging.class) != null || _findAnnotation(m, JsonIgnore.class) != null;
            }
        });

        return sensitiveMapper;
    }

    private static SimpleModule getDefaultSensitiveModule(){
        final SimpleModule simpleModule = new SimpleModule("SensitiveModule", new Version(1, 0, 0, null, null, null));

        simpleModule.addSerializer(String.class, new SensitiveStringValueSerializer());
        simpleModule.addSerializer(EncryptedField.class, new EncryptedFieldSerializer());
        simpleModule.addSerializer(LocalDate.class, new SensitiveLocalDateValueSerializer());
        simpleModule.addSerializer(BigDecimal.class, new SensitiveBigDecimalValueSerializer());
        simpleModule.addSerializer(Long.class, new SensitiveLongValueSerializer());
        simpleModule.addSerializer(Enum.class, new SensitiveEnumValueSerializer());

        return simpleModule;
    }

    @SuppressWarnings("unused")
    abstract static class AvroMixIn {
        @JsonIgnore
        abstract org.apache.avro.Schema getSchema();
        @JsonIgnore
        abstract org.apache.avro.specific.SpecificData getSpecificData();
    }
}
