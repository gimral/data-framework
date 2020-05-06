package leap.data.framework.extension.log4j2;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

public class LeapPropertiesClassLevelConfigurationTest {

    @ClassRule
    public static LoggerContextRule classLogLevelContext = new LoggerContextRule("log4j2-classLogLevel.properties");

    @Test
    public void testclassLogLevelConfiguration() {
        final Configuration config = classLogLevelContext.getConfiguration();
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
        LoggerConfig rootLogger = config.getRootLogger();
        assertEquals("Incorrect Log Level For Root Logger", Level.INFO, rootLogger.getLevel());
        final Logger logger = LogManager.getLogger(getClass());
        assertEquals("Incorrect Log Level For Class Logger", Level.DEBUG, logger.getLevel());
        logger.debug("Configuration works!!!");
    }
}
