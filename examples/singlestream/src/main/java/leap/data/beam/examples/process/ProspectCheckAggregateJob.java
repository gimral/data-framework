package leap.data.beam.examples.process;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;

import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.core.LeapDoFn;
import leap.data.beam.core.LeapParDo;
import leap.data.beam.examples.datatypes.AggregatedProspectCompany;

public class ProspectCheckAggregateJob {

    public static void main(String[] args) throws IOException{
        args[0] = "--inputTopics={\"topic1\":\"dev-topic1\"}";
        
        PipelineOptionsFactory.register(KafkaPipelineOptions.class);
        KafkaPipelineOptions options = PipelineOptionsFactory
                                        .fromArgs(args)
                                        .withValidation()
                                        .as(KafkaPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        
   
        
        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }                              
    }

    private static LeapDoFn<String, AggregatedProspectCompany> flatMaptoProspect() {
        
        return new LeapDoFn<String, AggregatedProspectCompany>() {

            private static final long serialVersionUID = 1285473094986748770L;

            @Override
            protected void innerProcessElement(String element,
                    DoFn<String, AggregatedProspectCompany>.ProcessContext c) {
                //process DLQ and Logging        
                c.output(new AggregatedProspectCompany());            
            }
        };
    }
}