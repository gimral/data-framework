package leap.data.framework.core.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import leap.data.framework.core.serialization.json.LeapObjectMapperProvider;
import leap.framework.core.exception.ExceptionConstants;
import leap.framework.core.exception.FrameworkException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventDataLogger {
    protected final Logger LOG = LoggerFactory.getLogger(EventDataLogger.class);
    //Todo: Make the mappers transient
    protected final ObjectMapper sensitiveMapper;
    protected final ObjectMapper sensitiveGenericMapper;

    public EventDataLogger(ObjectMapper sensitiveMapper,ObjectMapper sensitiveGenericMapper){
        this.sensitiveMapper = sensitiveMapper;
        this.sensitiveGenericMapper = sensitiveGenericMapper;
    }

    public EventDataLogger(){
        this(LeapObjectMapperProvider.getSensitiveObjectMapper(),
                LeapObjectMapperProvider.getGenericRecordSensistiveObjectMapper());
    }

    public void log(Object record){
        if(record instanceof SpecificRecord){
            log(record, sensitiveMapper);
        }
        else if(record instanceof GenericRecord){
            log(record, sensitiveGenericMapper);
        }
        else {
            log(record, sensitiveMapper);
        }
    }

    private void log(Object record, ObjectMapper sensitiveMapper){
        try {
            final String logMessage = sensitiveMapper.writeValueAsString(record);
            LOG.info(logMessage);
        } catch (JsonProcessingException e) {
            LOG.error("Error in event logging", e);
            throw new FrameworkException(ExceptionConstants.JSON_SERIALIZATION_EXCEPTION.errorCode, e);
        }
    }
}
