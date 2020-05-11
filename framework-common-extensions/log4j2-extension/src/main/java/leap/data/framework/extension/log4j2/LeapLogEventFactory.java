package leap.data.framework.extension.log4j2;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.ReusableLogEventFactory;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ReusableMessageFactory;

import java.util.List;

public class LeapLogEventFactory extends ReusableLogEventFactory {

    @Override
    public LogEvent createEvent(String loggerName, Marker marker, String fqcn, StackTraceElement location, Level level, Message message, List<Property> properties, Throwable t) {
        String formattedStr = message.getFormattedMessage();

        // write your code to manipulate your message here
        formattedStr = formattedStr.replaceAll("password", "*******");
        Message formattedMessage = ReusableMessageFactory.INSTANCE.newMessage(formattedStr);
        return super.createEvent(loggerName, marker, fqcn, location, level, formattedMessage, properties, t);
    }
}
