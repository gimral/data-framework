package leap.data.beam.examples.process;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
                                     .withBootstrapServers("localhost:9092")
                                     .withTopic("AggregateProspect")
                                     .withValueDeserializer(StringDeserializer.class)
                                     .withoutMetadata())
                                     .apply(Values.<String>create());   

        PCollection<ProspectCompany> agpc = input.apply("Map to prospect" ,
                                                      LeapParDo.of(new FlatMapToAggregateProspect()));

        PCollection<AggregatedProspectCompany> aggregatedProspectsCo = agpc                                              
                                                      .apply(Window.<ProspectCompany>
                                                             into(FixedWindows.of(Duration.standardSeconds(15))))
                                                      .apply(LeapParDo.of(new ProspectId()))
                                                      .apply(GroupByKey.create())
                                                      .apply(Values.create())
                                                      .apply(LeapParDo.of(new ReduceProspectCo()));


        PCollection<String> results = aggregatedProspectsCo.apply("Map to JSON" ,
                                                      LeapParDo.of(new AgggregateProspectToJSON()));                                              

                                      results.apply(KafkaIO.<Void,String>write()
                                                     .withBootstrapServers("localhost:9092")
                                                     .withTopic("ProspectAggregated")
                                                     .withValueSerializer(StringSerializer.class)
                                                     .values());
    }

    @Override
    public Class<KafkaPipelineOptions> getPipelineOptionsClass() {
        return KafkaPipelineOptions.class;
    }
}