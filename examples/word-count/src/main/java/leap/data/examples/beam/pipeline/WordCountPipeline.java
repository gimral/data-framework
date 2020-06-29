package leap.data.examples.beam.pipeline;

import leap.data.beam.pipeline.LeapBeamPipeline;
import leap.data.examples.beam.configuration.WordCountOptions;
import leap.data.examples.beam.transforms.CountWords;
import leap.data.examples.beam.transforms.FormatAsTextFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;

public class WordCountPipeline extends LeapBeamPipeline<WordCountOptions> {

    @Override
    public void createPipeline(Pipeline p) {
        p.apply("ReadLines", TextIO.read().from(pipelineOptions.getInputFile()))
                .apply("Count Words",new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(pipelineOptions.getOutput()));
    }

    @Override
    public Class<WordCountOptions> getPipelineOptionsClass() {
        return WordCountOptions.class;
    }
}
