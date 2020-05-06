package leap.data.framework.extension.log4j2;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.*;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

@Plugin(name = "CustomPropertiesConfigurationFactory", category = ConfigurationFactory.CATEGORY)
@Order(100)
public class LeapPropertiesConfigurationFactory extends PropertiesConfigurationFactory {
    private static final String DefaultAppenderType = "Console";
    private static final String DefaultAppenderName = "DEFAULT_STDOUT";
    private static final String DefaultLayout = "JsonLayout";

    private final Properties defaultProperties;

    //Constructor is called by the Log4j2 in runtime
    public LeapPropertiesConfigurationFactory() {
        defaultProperties = new Properties();
    }

    @Override
    public PropertiesConfiguration getConfiguration(LoggerContext loggerContext, ConfigurationSource source) {
        final Properties properties = new Properties();
        if(loggerContext.getObject("DoNotLoadConfigFile") == null) {
            try (final InputStream configStream = source.getInputStream()) {
                properties.load(configStream);
            } catch (final IOException ioe) {
                throw new ConfigurationException("Unable to load " + source.toString(), ioe);
            }
        }
        addDefaultPropertyValues(loggerContext);
        removeAppenderRefProperties(properties);

        properties.putAll(defaultProperties);

        return new PropertiesConfigurationBuilder()
                .setConfigurationSource(source)
                .setRootProperties(properties)
                .setLoggerContext(loggerContext)
                .build();
    }

    protected void addDefaultPropertyValue(String key, String value){
        defaultProperties.setProperty(key, value);
    }


    private void addDefaultPropertyValues(LoggerContext loggerContext){
        String appenderType = loggerContext.getObject("UseListAppender") == null ? DefaultAppenderType : "List";
        //Appender Properties
        addDefaultPropertyValue("appender.console.type",appenderType);
        addDefaultPropertyValue("appender.console.name",DefaultAppenderName);
        addDefaultPropertyValue("appender.console.layout.type",DefaultLayout);
        addDefaultPropertyValue("appender.console.layout.compact","true");
        addDefaultPropertyValue("appender.console.layout.eventEol","true");
        addDefaultPropertyValue("appender.console.layout.timestamp.type","KeyValuePair");
        addDefaultPropertyValue("appender.console.layout.timestamp.key","@timestamp");
        addDefaultPropertyValue("appender.console.layout.timestamp.value","$${date:yyyy-MM-dd'T'HH:mm:ss.SSSZ}");

        //Root Logger properties
        addDefaultPropertyValue("rootLogger.name", LoggerConfig.ROOT + "Logger");
        addDefaultPropertyValue("rootLogger.appenderRef.stdout.ref",DefaultAppenderName);
    }

    private void removeAppenderRefProperties(Properties properties){
        final ArrayList<String> removeAppenderList = new ArrayList<>();
        properties.forEach((key,value)->{
            if(((String)key).contains(".appenderRef.") && !value.toString().equals(DefaultAppenderName)){
                removeAppenderList.add((String)key);
            }
        });
        removeAppenderList.forEach((key)->{
            properties.remove(key);
        });
    }
}
