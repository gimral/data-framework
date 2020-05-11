package leap.data.framework.core.serialization.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotationMap;
import leap.framework.serialization.SensitiveContent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.lang.annotation.Annotation;

public class GenericRecordSerializer extends JsonSerializer<GenericRecord> {
    public GenericRecordSerializer(){}
    @Override
    public void serialize(GenericRecord record, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        final Schema schema = getRecordSchema(record);
        jsonGenerator.writeStartObject();
        for (Schema.Field f : schema.getFields()) {
            jsonGenerator.writeFieldName(f.name());
            final Object value = getField(record, f.pos());
            final String annotation = f.getProp("@javaAnnotation");

            BeanProperty property = null;
            if(annotation != null && annotation.equals("SensitiveContent")){
                final PropertyName propertyName = PropertyName.construct(f.getProp("name"));
                final AnnotatedMember annotatedMember = new AnnotatedField(null,null,
                        AnnotationMap.of(SensitiveContent.class, getAnnotation()));
                property = new BeanProperty.Std(propertyName,null,null,annotatedMember,null);
            }
            JsonSerializer<Object> ser = serializerProvider.findValueSerializer(value.getClass(),property);
            ser.serialize(value, jsonGenerator, serializerProvider);
        }
        jsonGenerator.writeEndObject();
    }

    public Object getField(Object record, int position) {
        return ((IndexedRecord)record).get(position);
    }

    protected Schema getRecordSchema(Object record) {
        return ((GenericContainer)record).getSchema();
    }

    public SensitiveContent getAnnotation() {
        return new SensitiveContent() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return SensitiveContent.class;
            }

            @Override
            public String maskChar() {
                return "*";
            }

        };
    }
}
