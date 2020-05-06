package leap.data.framework.extension.confluent.json;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.joda.time.LocalDate;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

public class JsonGenericDatumWriter {
    private final boolean quoteAllValues;

    public JsonGenericDatumWriter(){
        this(false);
    }
    public JsonGenericDatumWriter(boolean quoteAllValues){
        super();
        this.quoteAllValues = quoteAllValues;
    }
    /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
    public String write(Object datum) {
        StringBuilder buffer = new StringBuilder();
        toString(datum, buffer, new IdentityHashMap<Object, Object>(128) );
        return buffer.toString();
    }

    private static final String TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT =
            " \">>> CIRCULAR REFERENCE CANNOT BE PUT IN JSON STRING, ABORTING RECURSION <<<\" ";

    /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
    private void toString(Object datum, StringBuilder buffer, IdentityHashMap<Object, Object> seenObjects) {
        if (isRecord(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            Schema schema = getRecordSchema(datum);
            for (Schema.Field f : schema.getFields()) {
                toString(f.name(), buffer, seenObjects);
                buffer.append(": ");
                toString(getField(datum, f.name(), f.pos()), buffer, seenObjects);
                if (++count < schema.getFields().size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isArray(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            Collection<?> array = getArrayAsCollection(datum);
            buffer.append("[");
            long last = array.size()-1;
            int i = 0;
            for (Object element : array) {
                toString(element, buffer, seenObjects);
                if (i++ < last)
                    buffer.append(", ");
            }
            buffer.append("]");
            seenObjects.remove(datum);
        } else if (isMap(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            @SuppressWarnings(value="unchecked")
            Map<Object,Object> map = (Map<Object,Object>)datum;
            for (Map.Entry<Object,Object> entry : map.entrySet()) {
                toString(entry.getKey(), buffer, seenObjects);
                buffer.append(": ");
                toString(entry.getValue(), buffer, seenObjects);
                if (++count < map.size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isString(datum)|| isEnum(datum)) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (isBytes(datum)) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
            writeEscapedString(StandardCharsets.ISO_8859_1.decode(bytes), buffer);
            buffer.append("\"}");
        } else if (((datum instanceof Float) &&       // quote Nan & Infinity
                (((Float)datum).isInfinite() || ((Float)datum).isNaN()))
                || ((datum instanceof Double) &&
                (((Double)datum).isInfinite() || ((Double)datum).isNaN()))) {
            if (quoteAllValues) buffer.append("\"");
            buffer.append(datum);
            if (quoteAllValues) buffer.append("\"");
        } else if (datum instanceof GenericData) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            toString(datum, buffer, seenObjects);
            seenObjects.remove(datum);
        } else if (datum instanceof LocalDate){
            buffer.append("\"");
            buffer.append(datum);
            buffer.append("\"");
        }
        else {
            if (quoteAllValues) buffer.append("\"");
            buffer.append(datum);
            if (quoteAllValues) buffer.append("\"");
        }
    }

    /* Adapted from http://code.google.com/p/json-simple */
    private void writeEscapedString(CharSequence string, StringBuilder builder) {
        for(int i = 0; i < string.length(); i++){
            char ch = string.charAt(i);
            switch(ch){
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    // Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
                        String hex = Integer.toHexString(ch);
                        builder.append("\\u");
                        for(int j = 0; j < 4 - hex.length(); j++)
                            builder.append('0');
                        builder.append(hex.toUpperCase());
                    } else {
                        builder.append(ch);
                    }
            }
        }
    }

    protected boolean isEnum(Object datum) {
        return datum instanceof GenericEnumSymbol;
    }

    protected boolean isMap(Object datum) {
        return datum instanceof Map;
    }

    protected boolean isArray(Object datum) {
        return datum instanceof Collection;
    }

    protected boolean isString(Object datum) {
        return datum instanceof CharSequence;
    }

    protected boolean isBytes(Object datum) {
        return datum instanceof ByteBuffer;
    }

    protected Collection getArrayAsCollection(Object datum) {
        return (Collection)datum;
    }

    public Object getField(Object record, String name, int position) {
        return ((IndexedRecord)record).get(position);
    }

    protected boolean isRecord(Object datum) {
        return datum instanceof IndexedRecord;
    }

    protected Schema getRecordSchema(Object record) {
        return ((GenericContainer)record).getSchema();
    }
}
