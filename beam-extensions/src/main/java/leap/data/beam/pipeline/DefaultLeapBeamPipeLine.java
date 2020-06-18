package leap.data.beam.pipeline;

import leap.data.beam.configuration.LeapPipelineOptions;

public abstract class DefaultLeapBeamPipeLine extends LeapBeamPipeline<LeapPipelineOptions> {

    @Override
    public Class<LeapPipelineOptions> getPipelineOptionsClass() {
        return LeapPipelineOptions.class;
    }
}
