package leap.data.beam;

import leap.data.beam.transforms.join.OneToOneJoinTest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDataProvider {

    private static final Logger logger = LoggerFactory.getLogger(OneToOneJoinTest.class);

    @Rule
    public TestPipeline p = TestPipeline.create();

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
            "{\"name\":\"tran_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"tran_type\",\"type\":[\"null\", \"string\"]}," +
            "{\"name\":\"amount\",\"type\":[\"null\", \"double\"]}" +
            "]}";

    public static Schema TransactionDetailSchema = new Schema.Parser().parse(TransactionDetailSchemaStr);

    public static String TransactionHeaderSchemaStr = "" +
            "{" +
            "\"type\": \"record\"," +
            "\"name\": \"TransactionDetail\"," +
            "\"namespace\": \"leap.data.beam.entity\"," +
            "\"fields\":[" +
            "{\"name\":\"tran_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"tran_name\",\"type\":[\"null\", \"string\"]}" +
            "]}";

    public static Schema TransactionHeaderSchema = new Schema.Parser().parse(TransactionHeaderSchemaStr);

    public static GenericRecord getGenericAccount(long acid, long cust_id){
        if(acid<0)
            return null;
        return new GenericRecordBuilder(AccountSchema)
                .set("acid", acid)
                .set("cust_id", cust_id)
                .set("account_name", "Account " + acid)
                .set("balance", 100D)
                .build();
    }

    public static GenericRecord getGenericHeader(long tran_id){
        if(tran_id<0)
            return null;
        return new GenericRecordBuilder(TransactionHeaderSchema)
                .set("tran_id", tran_id)
                .set("tran_name", "Tran Name" + tran_id)
                .build();
    }

    public static GenericRecord getGenericTransactionDetail(long acid,long tran_id){
        if(tran_id<0 )
            return null;
        return new GenericRecordBuilder(TransactionDetailSchema)
                .set("tran_id", tran_id)
                .set("acid", acid)
                .set("tran_type", "TestType")
                .set("amount", 10D)
                .build();
    }

}
