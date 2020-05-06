package leap.data.framework.extension.confluent.schemaregistry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.LocalDate;

public class SerializerTestDataProvider {
    public static final String SCHEMA_STR_0 = avroSchemaString(0);
    public static final Schema AVRO_SCHEMA_0 = avroSchema(SCHEMA_STR_0);
    public static final String SCHEMA_STR_EVENT_ACCOUNT_CREATED = eventAccountCreatedSchemaString();
    public static final Schema AVRO_SCHEMA_EVENT_ACCOUNT_CREATED = avroSchema(SCHEMA_STR_EVENT_ACCOUNT_CREATED);
    public static final String JSON_DATA_EVENT_ACCOUNT_CREATED = jsonDataEventAccountCreated();
    public static final GenericRecord GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED = genericRecordDataEventAccountCreated();

    private static Schema avroSchema(final String avroSchemaStr) {
        return new Schema.Parser().parse(avroSchemaStr);
    }

    private static String avroSchemaString(final int i) {
        return "{\"type\": \"record\", \"name\": \"Blah" + i + "\", "
                + "\"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}";
    }

    private static String eventAccountCreatedSchemaString() {
        return "\n" +
                "{\n" +
                "\"type\": \"record\",\n" +
                "\"name\": \"AccountCreated\",\n" +
                "\"fields\":[\n" +
                "{\"name\":\"eventId\",\"type\":[\"null\", \"long\"]},\n" +
                "{\"name\":\"traceId\",\"type\":[\"null\", \"long\"]},\n" +
                "{\"name\":\"type\",\"type\":[\"null\", \"string\"]},\n" +
                "{\"name\":\"data\",\"type\":\n" +
                "{\n" +
                "\"type\":\"record\",\n" +
                "\"name\":\"DataType\",\n" +
                "\"fields\":[\n" +
                "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]},\n" +
                //"{\"name\":\"openingdate\",\"type\":[\"null\", \"long\"]},\n" +
                "{\"name\":\"openingdate\",\"type\":[\"null\", {\"type\":\"int\", \"logicalType\": \"date\"}]},\n" +
                "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}\n" +
                "]\n" +
                "}\n" +
                "}]}";
    }

    private static String jsonDataEventAccountCreated() {
        return "{\"eventId\": 231232131, \"traceId\": 2131231231, \"type\": \"AccountCreated\", \"data\": {\"acid\": 11, \"openingdate\": \"2020-02-15\", \"balance\": 100.0}}";
    }

    private static GenericRecord genericRecordDataEventAccountCreated() {
        return new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_CREATED)
                .set("eventId", 231232131L)
                .set("traceId", 2131231231L)
                .set("type", "AccountCreated")
                .set("data",new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_CREATED.getField("data").schema())
                        .set("acid",11L)
                        .set("openingdate",new LocalDate(2020,2,15))
                        .set("balance",100D)
                        .build())
                .build();
    }
}
