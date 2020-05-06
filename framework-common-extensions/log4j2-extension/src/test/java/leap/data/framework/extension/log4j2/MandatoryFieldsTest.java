package leap.data.framework.extension.log4j2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MandatoryFieldsTest {

    @ClassRule
    public static LoggerContextRule loggerContext = new LoggerContextRule();

    @Test
    public void testLogContainsMandatoryFields() throws JsonProcessingException {

        loggerContext.getLoggerContext().putObject("UseListAppender","1");
        loggerContext.reconfigure();
        final ListAppender appender = loggerContext.getListAppender("DEFAULT_STDOUT");

        final Logger logger = LogManager.getLogger(getClass());
        logger.error("Error Log");

        final List<String> messages = appender.getMessages();
        assertNotNull(messages);
        assertEquals("Incorrect number of messages", 1, messages.size());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonMessage = mapper.readTree(messages.get(0));
        assertNotNull("No @timestamp field", jsonMessage.get("@timestamp"));
        assertNotNull("No message field", jsonMessage.get("message"));
        assertNotNull("No level field", jsonMessage.get("level"));
        //assertNotNull("No level field", jsonMessage.get("level"));
    }
}
