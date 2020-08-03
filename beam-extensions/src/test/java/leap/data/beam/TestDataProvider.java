package leap.data.beam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class TestDataProvider {
    public static String AccountSchemaStr = "" +
                "{" +
                "\"type\": \"record\"," +
                "\"name\": \"Account\"," +
                "\"namespace\": \"leap.data.beam.entity\"," +
                "\"fields\":[" +
                "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"cust_id\",\"type\":[\"null\", \"long\"]}," +
                "{\"name\":\"account_name\",\"type\":[\"null\", \"string\"]}," +
                "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}" +
                "]}";

    public static Schema AccountSchema = new Schema.Parser().parse(AccountSchemaStr);

    public static String TransactionDetailSchemaStr = "" +
            "{" +
            "\"type\": \"record\"," +
            "\"name\": \"TransactionDetail\"," +
            "\"namespace\": \"leap.data.beam.entity\"," +
            "\"fields\":[" +
            "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"tran_type\",\"type\":[\"null\", \"string\"]}," +
            "{\"name\":\"amount\",\"type\":[\"null\", \"double\"]}" +
            "]}";

    public static Schema TransactionDetailSchema = new Schema.Parser().parse(TransactionDetailSchemaStr);

    public static GenericRecord getGenericAccount(long acid, long cust_id){
        return new GenericRecordBuilder(AccountSchema)
                .set("acid", acid)
                .set("cust_id", cust_id)
                .set("account_name", "Account " + acid)
                .set("balance", 100D)
                .build();
    }

    public static GenericRecord getGenericTransactionDetail(long acid){
        return new GenericRecordBuilder(TransactionDetailSchema)
                .set("acid", acid)
                .set("tran_type", "TestType")
                .set("amount", 10D)
                .build();
    }

}