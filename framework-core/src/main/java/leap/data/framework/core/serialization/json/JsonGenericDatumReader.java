package leap.data.framework.core.serialization.json;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.WeakIdentityHashMap;

import java.io.IOException;
import java.util.Map;

@SuppressWarnings({"unchecked", "FieldMayBeFinal"})
public class JsonGenericDatumReader<D> extends GenericDatumReader<D> {
    private GenericData data;
    private Schema actual;
    private Schema expected;
    private JsonResolvingDecoder creatorResolver;
    private Thread creator;

    private static final ThreadLocal<Map<Schema, Map<Schema, JsonResolvingDecoder>>> RESOLVER_CACHE = new ThreadLocal<Map<Schema, Map<Schema, JsonResolvingDecoder>>>() {
        protected Map<Schema, Map<Schema, JsonResolvingDecoder>> initialValue() {
            return new WeakIdentityHashMap();
        }
    };
    private Decoder decoder;

    public JsonGenericDatumReader() {
        this(null,null, GenericData.get());
    }

    public JsonGenericDatumReader(Schema schema) {
        this(schema, schema, GenericData.get());
    }

    public JsonGenericDatumReader(Schema writer, Schema reader) {
        this(writer, reader, GenericData.get());
    }

    public JsonGenericDatumReader(Schema writer, Schema reader, GenericData data) {
        super(writer, reader, data);
        this.actual = writer;
        this.expected = reader;
        configure(data);
    }

    private void configure(GenericData data) {
        this.creatorResolver = null;
        this.data = data;
        this.creator = Thread.currentThread();
    }
//
    protected final JsonResolvingDecoder getJsonResolver(Schema actual, Schema expected) throws IOException {
        Thread currThread = Thread.currentThread();
        if (currThread == creator && creatorResolver != null) {
            return creatorResolver;
        } else {
            Map<Schema, JsonResolvingDecoder> cache = (Map)((Map)RESOLVER_CACHE.get()).get(actual);
            if (cache == null) {
                cache = new WeakIdentityHashMap();
                ((Map)RESOLVER_CACHE.get()).put(actual, cache);
            }

            JsonResolvingDecoder resolver = (JsonResolvingDecoder)((Map)cache).get(expected);
            if (resolver == null) {
                //TODO: Generate factory for this
                resolver = new JsonResolvingDecoder(Schema.applyAliases(actual, expected), expected, null);//DecoderFactory.get().resolvingDecoder(Schema.applyAliases(actual, expected), expected, null);
                ((Map)cache).put(expected, resolver);
            }

            if (currThread == creator) {
                creatorResolver = resolver;
            }

            return resolver;
        }
    }


    @Override
    public D read(D reuse, Decoder in) throws IOException {
        JsonResolvingDecoder resolver = this.getJsonResolver(this.actual, this.expected);
        resolver.configure(in);
        D result = (D)this.read(reuse, this.expected, resolver);
        resolver.drain();
        return result;
    }


//    protected Object read(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
//        LogicalType logicalType = expected.getLogicalType();
//        Conversion<?> conversion = null;
//        Object datum = null;
//        if (logicalType != null) {
//            conversion = getData().getConversionFor(logicalType);
//            if(logicalType.getName() == "date") {
//                String rawDatum = (String)this.readString(old, expected, in);
//                datum = (int) LocalDate.parse(rawDatum, DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay();
//            }
//        }
//        datum = this.readWithoutConversion(old, expected, in);
//        if (conversion != null) {
//            return convert(datum, expected, logicalType, conversion);
//        }
//
//        return datum;
//    }


    protected Object read(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
        Object datum = this.readWithoutConversion(old, expected, in);
        LogicalType logicalType = expected.getLogicalType();
        if (logicalType != null) {
            Conversion<?> conversion = this.getData().getConversionFor(logicalType);
            if (conversion != null) {
                return this.convert(datum, expected, logicalType, conversion);
            }
        }

        return datum;
    }

    protected Object readWithConversion(Object old, Schema expected, LogicalType logicalType, Conversion<?> conversion, JsonResolvingDecoder in) throws IOException {
        return this.convert(this.readWithoutConversion(old, expected, in), expected, logicalType, conversion);
    }

    protected Object readWithoutConversion(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
        LogicalType logicalType = expected.getLogicalType();
        if (logicalType != null) {
            if(logicalType.getName() == "date"){
                return in.readEpoch();
            }
        }
        switch(expected.getType()) {
            case RECORD:
                return this.readRecord(old, expected, in);
            case ENUM:
                return this.readEnum(expected, in);
            case ARRAY:
                return this.readArray(old, expected, in);
            case MAP:
                return this.readMap(old, expected, in);
            case UNION:
                return this.read(old, (Schema)expected.getTypes().get(in.readIndex()), in);
            case FIXED:
                return this.readFixed(old, expected, in);
            case STRING:
                return this.readString(old, expected, in);
            case BYTES:
                return this.readBytes(old, expected, in);
            case INT:
                return this.readInt(old, expected, in);
            case LONG:
                return in.readLong();
            case FLOAT:
                return in.readFloat();
            case DOUBLE:
                return in.readDouble();
            case BOOLEAN:
                return in.readBoolean();
            case NULL:
                in.readNull();
                return null;
            default:
                throw new AvroRuntimeException("Unknown type: " + expected);
        }
    }

    protected Object readRecord(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
        Object r = this.data.newRecord(old, expected);
        //Object state = this.data.getRecordState(r, expected);
        Schema.Field[] arr$ = in.readFieldOrder();
        int len$ = arr$.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            Schema.Field f = arr$[i$];
            int pos = f.pos();
            String name = f.name();
            Object oldDatum = null;
            if (old != null) {
                //oldDatum = this.data.getField(r, name, pos, state);
                oldDatum = this.data.getField(r, name, pos);
            }

            this.readField(r, f, oldDatum, in);
        }

        return r;
    }

    protected void readField(Object r, Schema.Field f, Object oldDatum, JsonResolvingDecoder in) throws IOException {
        this.data.setField(r, f.name(), f.pos(), this.read(oldDatum, f.schema(), in));
    }

    protected Object readArray(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
        Schema expectedType = expected.getElementType();
        long l = in.readArrayStart();
        long base = 0L;
        if (l <= 0L) {
            return this.newArray(old, 0, expected);
        } else {
            LogicalType logicalType = expectedType.getLogicalType();
            Conversion<?> conversion = this.getData().getConversionFor(logicalType);
            Object array = this.newArray(old, (int)l, expected);

            do {
                long i;
                if (logicalType != null && conversion != null) {
                    for(i = 0L; i < l; ++i) {
                        this.addToArray(array, base + i, this.readWithConversion(this.peekArray(array), expectedType, logicalType, conversion, in));
                    }
                } else {
                    for(i = 0L; i < l; ++i) {
                        this.addToArray(array, base + i, this.readWithoutConversion(this.peekArray(array), expectedType, in));
                    }
                }

                base += l;
            } while((l = in.arrayNext()) > 0L);

            return array;
        }
    }

    protected Object readMap(Object old, Schema expected, JsonResolvingDecoder in) throws IOException {
        Schema eValue = expected.getValueType();
        long l = in.readMapStart();
        LogicalType logicalType = eValue.getLogicalType();
        Conversion<?> conversion = this.getData().getConversionFor(logicalType);
        Object map = this.newMap(old, (int)l);
        if (l > 0L) {
            do {
                int i;
                if (logicalType != null && conversion != null) {
                    for(i = 0; (long)i < l; ++i) {
                        this.addToMap(map, this.readMapKey((Object)null, expected, in), this.readWithConversion((Object)null, eValue, logicalType, conversion, in));
                    }
                } else {
                    for(i = 0; (long)i < l; ++i) {
                        this.addToMap(map, this.readMapKey((Object)null, expected, in), this.readWithoutConversion((Object)null, eValue, in));
                    }
                }
            } while((l = in.mapNext()) > 0L);
        }

        return map;
    }

}
