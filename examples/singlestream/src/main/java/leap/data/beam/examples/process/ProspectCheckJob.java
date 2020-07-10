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

public class ProspectCheckJob {

    public static void main(String[] args) throws IOException{
        args[0] = "--inputTopics={\"topic1\":\"dev-topic1\"}";
        
        PipelineOptionsFactory.register(KafkaPipelineOptions.class);
        KafkaPipelineOptions options = PipelineOptionsFactory
                                        .fromArgs(args)
                                        .withValidation()
                                        .as(KafkaPipelineOptions.class);

                                        
                                        
    }    
}