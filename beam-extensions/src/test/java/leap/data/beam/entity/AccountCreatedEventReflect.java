package leap.data.beam.entity;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@SuppressWarnings("unused")
@DefaultSchema(JavaBeanSchema.class)
public class AccountCreatedEventReflect {
    private Long eventId;
    private Long traceId;
    private String type;
    private AccountReflect data;

    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

    public Long getTraceId() {
        return traceId;
    }

    public void setTraceId(Long traceId) {
        this.traceId = traceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public AccountReflect getData() {
        return data;
    }

    public void setData(AccountReflect data) {
        this.data = data;
    }
}
