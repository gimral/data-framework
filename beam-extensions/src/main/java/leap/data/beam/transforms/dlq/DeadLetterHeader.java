package leap.data.beam.transforms.dlq;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
public class DeadLetterHeader {
    private String originSource;
    private Integer originPartition;
    private Long originOffset;
    private String originId;
    private String applicationName;
    private String errorCategory;
    private String errorReason;
    private String errorDescription;
    private Integer retryCount;
    private String expectedSchema;
    private String actualSchema;

    public String getOriginSource() {
        return originSource;
    }

    public void setOriginSource(String originSource) {
        this.originSource = originSource;
    }

    public Integer getOriginPartition() {
        return originPartition;
    }

    public void setOriginPartition(Integer originPartition) {
        this.originPartition = originPartition;
    }

    public Long getOriginOffset() {
        return originOffset;
    }

    public void setOriginOffset(Long originOffset) {
        this.originOffset = originOffset;
    }

    public String getOriginId() {
        return originId;
    }

    public void setOriginId(String originId) {
        this.originId = originId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getErrorCategory() {
        return errorCategory;
    }

    public void setErrorCategory(String errorCategory) {
        this.errorCategory = errorCategory;
    }

    public String getErrorReason() {
        return errorReason;
    }

    public void setErrorReason(String errorReason) {
        this.errorReason = errorReason;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription) {
        this.errorDescription = errorDescription;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public String getExpectedSchema() {
        return expectedSchema;
    }

    public void setExpectedSchema(String expectedSchema) {
        this.expectedSchema = expectedSchema;
    }

    public String getActualSchema() {
        return actualSchema;
    }

    public void setActualSchema(String actualSchema) {
        this.actualSchema = actualSchema;
    }
}
