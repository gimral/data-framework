package leap.data.beam.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class AvroTestDataProvider {
    public static final String SCHEMA_STR_EVENT_ACCOUNT_CREATED = eventAccountCreatedSchemaString();
    public static final String SCHEMA_STR_EVENT_ACCOUNT_BALANCE_UPDATED = eventAccountBalanceUpdatedString();
    public static final String READER_SCHEMA_STR_ACCOUNT = eventAccountReaderSchemaString();
    public static final Schema AVRO_SCHEMA_EVENT_ACCOUNT_CREATED = avroSchema(SCHEMA_STR_EVENT_ACCOUNT_CREATED);
    public static final Schema AVRO_SCHEMA_EVENT_ACCOUNT_BALANCE_UPDATED = avroSchema(SCHEMA_STR_EVENT_ACCOUNT_BALANCE_UPDATED);
    public static final Schema AVRO_READER_SCHEMA_EVENT_ACCOUNT = avroSchema(READER_SCHEMA_STR_ACCOUNT);

    private static Schema avroSchema(final String avroSchemaStr) {
        return new Schema.Parser().parse(avroSchemaStr);
    }


    private static String eventAccountCreatedSchemaString() {
        return "" +
                "{" +
                "\"type\": \"record\"," +
                "\"name\": \"AccountCreatedEvent\"," +
                "\"namespace\": \"leap.data.beam.entity\"," +
                "\"fields\":[" +
                "{\"name\":\"eventId\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"traceId\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"type\",\"type\":[\"null\", \"string\"]}," +
                "{\"name\":\"data\",\"type\":" +
                "{" +
                "\"type\":\"record\"," +
                "\"name\":\"Account\"," +
                "\"namespace\": \"leap.data.beam.entity\"," +
                "\"fields\":[" +
                "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
                //"{\"name\":\"openingdate\",\"type\":[\"null\", {\"type\":\"int\", \"logicalType\": \"date\"}]}," +
                "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}" +
                "]" +
                "}" +
                "}]}";
    }

    private static String eventAccountBalanceUpdatedString() {
        return "" +
                "{" +
                "\"type\": \"record\"," +
                "\"name\": \"AccountBalanceUpdatedEvent\"," +
                "\"namespace\": \"leap.data.beam.entity\"," +
                "\"fields\":[" +
                "{\"name\":\"eventId\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"traceId\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"type\",\"type\":[\"null\", \"string\"]}," +
                "{\"name\":\"data\",\"type\":" +
                "{" +
                "\"type\":\"record\"," +
                "\"name\":\"AccountBalance\"," +
                "\"namespace\": \"leap.data.beam.entity\"," +
                "\"fields\":[" +
                "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"previous_balance\",\"type\":[\"null\", \"double\"]}," +
                "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}" +
                "]" +
                "}" +
                "}]}";
    }

    private static String eventAccountReaderSchemaString() {
        return "" +
                "{" +
                "\"type\": \"record\"," +
                "\"name\": \"AccountCreatedEvent\"," +
                "\"fields\":[" +
                "{\"name\":\"eventId\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"data\",\"type\":" +
                "{" +
                "\"type\":\"record\"," +
                "\"name\":\"DataType\"," +
                "\"fields\":[" +
                "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}" +
                "]" +
                "}" +
                "}]}";
    }

    public static GenericRecord genericRecordDataEventAccountCreated(Long eventId) {
        return new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_CREATED)
                .set("eventId", eventId)
                .set("traceId", 2131231231L)
                .set("type", "AccountCreated")
                .set("data",new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_CREATED.getField("data").schema())
                        .set("acid",11L)
                        //.set("openingdate",new LocalDate(2020,2,15))
                        .set("balance",100D)
                        .build())
                .build();
    }

    public static GenericRecord genericRecordDataEventAccountBalanceUpdated(Long eventId) {
        return new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_BALANCE_UPDATED)
                .set("eventId", eventId)
                .set("traceId", 2131231231L)
                .set("type", "AccountCreated")
                .set("data",new GenericRecordBuilder(AVRO_SCHEMA_EVENT_ACCOUNT_BALANCE_UPDATED.getField("data").schema())
                        .set("acid",11L)
                        .set("previous_balance",90D)
                        .set("balance",100D)
                        .build())
                .build();
    }
}
