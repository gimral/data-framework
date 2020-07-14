package leap.data.beam.examples.process;

import java.io.IOException;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

import leap.data.beam.configuration.KafkaPipelineOptions;

public class ProspectCheckJob {

    public static void main(String[] args) throws IOException{
        //args[0] = "--inputTopics={\"topic1\":\"dev-topic1\"}";
        
        //PipelineOptionsFactory.register(KafkaPipelineOptions.class);
        /*KafkaPipelineOptions options = PipelineOptionsFactory
                                        .fromArgs(args)
                                        .withValidation()
                                        .as(KafkaPipelineOptions.class);*/

        ProspectCheckPipleline p = new ProspectCheckPipleline();
        p.run(args);                                
    }    
}