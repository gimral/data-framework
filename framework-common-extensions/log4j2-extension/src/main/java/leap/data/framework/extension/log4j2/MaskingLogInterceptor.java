package leap.data.framework.extension.log4j2;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.rewrite.RewritePolicy;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.StringMap;

@Plugin(name = "MaskingLogInterceptor", category = "Core", elementType = "rewritePolicy", printObject = true)
public class MaskingLogInterceptor implements RewritePolicy {
    @Override
    public LogEvent rewrite(LogEvent source) {
        String message = source.getMessage().getFormattedMessage();

        // write your code to manipulate your message here
        message = message.replaceAll("password", "*******");

        return Log4jLogEvent.newBuilder()
                .setLoggerName(source.getLoggerName())
                .setMarker(source.getMarker())
                .setLoggerFqcn(source.getLoggerFqcn())
                .setLevel(source.getLevel())
                .setMessage(new SimpleMessage(message))
                .setThrown(source.getThrown())
                .setContextData((StringMap)source.getContextData())
                .setContextStack(source.getContextStack())
                .setThreadName(source.getThreadName())
                .setSource(source.getSource())
                .setTimeMillis(source.getTimeMillis())
                .build();
    }

    @PluginFactory
    public static MaskingLogInterceptor createPolicy() {
        return new MaskingLogInterceptor();
    }

}
