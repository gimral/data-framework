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

import static org.assertj.core.api.Assertions.assertThat;

public class LeapPropertiesClassLevelConfigurationTest {

    @ClassRule
    public static LoggerContextRule classLogLevelContext = new LoggerContextRule("log4j2-classLogLevel.properties");

    @Test
    public void testclassLogLevelConfiguration() {
        //when:
        final Configuration config = classLogLevelContext.getConfiguration();
        //then:
        assertThat(config).as("No configuration created!").isNotNull();
        assertThat(config.getState()).as("Incorrect State!").isEqualTo(LifeCycle.State.STARTED);

        //when:
        final Map<String, Appender> appenders = config.getAppenders();
        //then:
        assertThat(appenders).as("No appenders created!").isNotNull();
        assertThat(appenders).as("Incorrect number of Appenders!").hasSize(1);

        //when:
        @SuppressWarnings("rawtypes")
        final Layout layout = appenders.entrySet().iterator().next().getValue().getLayout();
        //then:
        assertThat(layout).as("No layout created!").isNotNull();
        assertThat(layout).as("Incorrect layout!").isInstanceOf(JsonLayout.class);

        //when:
        final Map<String, LoggerConfig> loggers = config.getLoggers();
        //then:
        assertThat(loggers).as("No loggers created!").isNotNull();

        //when:
        LoggerConfig rootLogger = config.getRootLogger();
        //then:
        assertThat(rootLogger.getLevel()).as("Incorrect Log Level For Root Logger!").isEqualTo(Level.INFO);

        //when:
        final Logger logger = LogManager.getLogger(getClass());
        //then:
        assertThat(logger.getLevel()).as("Incorrect Log Level For Class Logger!").isEqualTo(Level.DEBUG);
        logger.debug("Configuration works!!!");
    }
}
