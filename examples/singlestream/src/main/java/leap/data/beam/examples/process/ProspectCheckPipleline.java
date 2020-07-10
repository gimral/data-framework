package leap.data.beam.examples.process;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.core.LeapParDo;
import leap.data.beam.examples.datatypes.AggregatedProspectCompany;
import leap.data.beam.examples.datatypes.ProspectCompany;
import leap.data.beam.pipeline.LeapBeamPipeline;

public class ProspectCheckPipleline extends LeapBeamPipeline<KafkaPipelineOptions> {

    @Override
    public void createPipeline(Pipeline p) {

        PCollection<String> input = p.apply(KafkaIO.<Long,String>read()
                                     .withBootstrapServers("kafka:29092")
                                     .withTopic("AggregateProspect")
                                     .withValueDeserializer(StringDeserializer.class)
                                     .withoutMetadata())
                                     .apply(Values.<String>create());   

        PCollection<ProspectCompany> agpc = input.apply("Map to prospect" ,
                                                      LeapParDo.of(new FlatMapToAggregateProspect()));

        PCollection<ProspectCompany> aggregatedProspectsCo = agpc                                              
                                                      .apply(Window.<ProspectCompany>
                                                             into(FixedWindows.of(Duration.standardSeconds(15))))
                                                      .apply(LeapParDo.of(new ProspectId()))
                                                      .apply(GroupByKey.create())
                                                      .apply(Values.create())
                                                      .apply(Flatten.iterables());
                                                      //.apply(ParDo.of(new ReduceProspectCo()));
                                                      
    }

    @Override
    public Class<KafkaPipelineOptions> getPipelineOptionsClass() {
        return KafkaPipelineOptions.class;
    }
}