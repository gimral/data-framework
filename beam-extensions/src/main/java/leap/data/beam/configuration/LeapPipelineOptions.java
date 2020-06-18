package leap.data.beam.configuration;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Description;

@SuppressWarnings("unused")
public interface LeapPipelineOptions extends FlinkPipelineOptions {
    @Description("Entity")
    String getEntity();
    void setEntity(String entity);
}

