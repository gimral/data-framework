package leap.data.framework.extension.log4j2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class MandatoryFieldsTest {

    @ClassRule
    public static LoggerContextRule loggerContext = new LoggerContextRule();

    @Test
    public void testLogContainsMandatoryFields() throws JsonProcessingException {
        //given:
        loggerContext.getLoggerContext().putObject("UseListAppender","1");
        loggerContext.reconfigure();
        final ListAppender appender = loggerContext.getListAppender("DEFAULT_STDOUT");
        final Logger logger = LogManager.getLogger(getClass());

        //when:
        logger.error("Error Log");

        //then:
        final List<String> messages = appender.getMessages();
        assertThat(messages).isNotNull();
        assertThat(messages).as("Incorrect number of messages!").hasSize(1);
        assertNotNull(messages);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonMessage = mapper.readTree(messages.get(0));

        assertThat(jsonMessage.get("@timestamp")).as("No @timestamp field").isNotNull();
        assertThat(jsonMessage.get("message")).as("No message field").isNotNull();
        assertThat(jsonMessage.get("level")).as("No level field").isNotNull();
    }
}
