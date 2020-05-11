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
import static org.junit.Assert.*;

public class LeapPropertiesJsonLayoutConfigurationTest {

    @ClassRule
    public static LoggerContextRule patternLayoutContext = new LoggerContextRule("log4j2-patternLayout.properties");

    @Test
    public void testJsonLayoutEnforced() {
        //when:
        final Configuration config = patternLayoutContext.getConfiguration();
        //then:
        assertThat(config).as("No configuration created!").isNotNull();
        assertThat(config.getState()).as("Incorrect State!").isEqualTo(LifeCycle.State.STARTED);

        config.getLoggers().forEach((loggerName,loggerConfig)-> loggerConfig.getAppenders().forEach((appenderName, appender)->{
            final Layout layout = appender.getLayout();

            assertThat(layout).as("No layout created for logger: " + loggerName).isNotNull();
            assertThat(layout).as("Incorrect layout for logger: " + loggerName)
                    .isInstanceOf(JsonLayout.class);
        }));

        final Logger logger = LogManager.getLogger(getClass());
        logger.error("Configuration works!!!");
    }
}
