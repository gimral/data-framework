package leap.data.framework.core.logging;

import com.google.common.collect.ImmutableList;
import leap.data.framework.core.test.AccountCreated;
import leap.data.framework.core.test.AccountCreatedData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class EventDataLoggerTest {
    private static final TestLogger logger = TestLoggerFactory.getTestLogger(EventDataLogger.class);
    @Test
    public void testLogSpecificRecordEventData(){
        //given:
        EventDataLogger eventDataLogger = new EventDataLogger();
        AccountCreated accountCreated = new AccountCreated();
        accountCreated.setEventId(1L);
        accountCreated.setTraceId(1L);
        accountCreated.setType("AccountCreated");
        AccountCreatedData accountCreatedData = new AccountCreatedData();
        accountCreatedData.setAcid(1L);
        accountCreatedData.setCid(1L);
        accountCreatedData.setBalance(1.0D);
        accountCreatedData.setOpeningdate(new LocalDate(2001,1,1));
        accountCreated.setData(accountCreatedData);
        //when:
        eventDataLogger.log(accountCreated);
        //then:
        ImmutableList<LoggingEvent> loggingEvents = logger.getLoggingEvents();
        assertThat(loggingEvents).hasSize(1)
                .allSatisfy((loggingEvent -> assertThat(loggingEvent.getMessage()).isEqualTo(" Record:{\"eventId\":\"1\",\"traceId\":\"1\"," +
                        "\"type\":\"AccountCreated\",\"data\":" +
                        "{\"acid\":\"*\",\"cid\":\"1\",\"openingdate\":\"2001-01-01\",\"balance\":1.0}}")));
    }

    @Test
    public void testLogGenericRecordEventData() throws IOException {
        //given:
        EventDataLogger eventDataLogger = new EventDataLogger();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream schemainputStream = loader.getResourceAsStream("avro/AccountCreatedSchema.avsc");
        Schema schema = new Schema.Parser().parse(schemainputStream);
        GenericRecord genericRecord = new GenericRecordBuilder(schema)
                .set("eventId", 1L)
                .set("traceId", 1L)
                .set("type", "AccountCreated")
                .set("data",new GenericRecordBuilder(schema.getField("data").schema())
                        .set("acid",1L)
                        .set("cid",1L)
                        .set("openingdate",new LocalDate(2001,1,1))
                        .set("balance",1.0D)
                        .build())
                .build();

        //when:
        eventDataLogger.log(genericRecord);
        //then:
        ImmutableList<LoggingEvent> loggingEvents = logger.getLoggingEvents();
        assertThat(loggingEvents).hasSize(1)
                .allSatisfy((loggingEvent -> assertThat(loggingEvent.getMessage()).isEqualTo(" Record:{\"eventId\":\"1\",\"traceId\":\"1\"," +
                        "\"type\":\"AccountCreated\",\"data\":" +
                        "{\"acid\":\"*\",\"cid\":\"1\",\"openingdate\":\"2001-01-01\",\"balance\":1.0}}")));
    }

    @Test
    public void testPrimitiveTypeEventData(){
        //given:
        EventDataLogger eventDataLogger = new EventDataLogger();
        //when:
        HashMap<Object,Object> primitives = new HashMap<>();
        primitives.put(null,"null");
        primitives.put(1,"1");
        primitives.put(1L,"\"1\"");//TODO: Is this the expected behavior
        primitives.put(1.1f,"1.1");
        primitives.put(1.1d,"1.1");
        primitives.put("","\"\"");
        primitives.put("abc","\"abc\"");
        //primitives.put("abc".getBytes(),"abc".getBytes().toString());
        primitives.forEach((key,value) -> eventDataLogger.log(key));
        //then:
        ImmutableList<LoggingEvent> loggingEvents = logger.getLoggingEvents();
        assertThat(loggingEvents).hasSize(primitives.size());
        for (int i = 0; i < loggingEvents.size(); i++) {
            assertThat(loggingEvents.get(i).getMessage()).isEqualTo(" Record:" + primitives.values().toArray()[i]);
        }
    }

    @After
    public void clear(){
        TestLoggerFactory.clear();
    }


}
