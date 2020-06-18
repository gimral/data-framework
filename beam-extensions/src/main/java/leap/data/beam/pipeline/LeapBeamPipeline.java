package leap.data.beam.pipeline;

import leap.data.beam.configuration.LeapPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public abstract class LeapBeamPipeline<T extends LeapPipelineOptions> {
    protected T pipelineOptions;

    public void run(String[] args){
        Class<T> pipelineOptionsClass =  getPipelineOptionsClass();
        PipelineOptionsFactory.register(pipelineOptionsClass);
        pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(pipelineOptionsClass);
        Pipeline p = Pipeline.create(pipelineOptions);
        createPipeline(p);
        p.run().waitUntilFinish();
    }

    public abstract void createPipeline(Pipeline p);

    public abstract Class<T> getPipelineOptionsClass();
}
