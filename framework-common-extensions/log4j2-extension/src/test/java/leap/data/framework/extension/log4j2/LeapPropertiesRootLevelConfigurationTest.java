package leap.data.framework.extension.log4j2;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class LeapPropertiesRootLevelConfigurationTest {

    @ClassRule
    public static LoggerContextRule rootLogLevelContext = new LoggerContextRule("log4j2-rootLogLevel.properties");


    @Test
    public void testrootLogLevelConfiguration() {
        final Configuration config = rootLogLevelContext.getConfiguration();
        assertNotNull("No configuration created", config);
        assertEquals("Incorrect State: " + config.getState(), config.getState(), LifeCycle.State.STARTED);
        final Map<String, Appender> appenders = config.getAppenders();
        assertNotNull(appenders);
        assertEquals("Incorrect number of Appenders", 1, appenders.size());
        final Layout layout = appenders.entrySet().iterator().next().getValue().getLayout();
        assertNotNull("No layout created", layout);
        assertTrue("Incorrect Layout: " + layout.getClass().getName(), layout instanceof JsonLayout);
        final Map<String, LoggerConfig> loggers = config.getLoggers();
        assertNotNull(loggers);
        assertEquals("Incorrect number of LoggerConfigs", 1, loggers.size());
        assertEquals("Incorrect Log Level", Level.INFO, loggers.entrySet().iterator().next().getValue().getLevel());
        final Logger logger = LogManager.getLogger(getClass());
        logger.info("Configuration works!!!");
    }
}
