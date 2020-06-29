package leap.data.examples.beam.transforms;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.core.LeapParDo;
import leap.data.beam.core.LogEventData;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that converts a PCollection containing lines of text into a PCollection of
 * formatted word counts.
 *
 * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
 * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
 * modular testing, and an improved monitoring experience.
 */
public class CountWords
        extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    private final Logger logger = LoggerFactory.getLogger(CountWords.class);
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
        // Convert lines of text into individual words.
        PCollection<String> words = lines.apply("Extract Words",LeapParDo.of(new ExtractWordsFn()));

        // Count the number of times each word occurs.
        PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

        return wordCounts;
    }

    @LogEventData
    static class ExtractWordsFn extends LeapDoFn<String, String> {
        private final Logger logger = LoggerFactory.getLogger(ExtractWordsFn.class);
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");


        @Override
        protected void innerProcessElement(String element, ProcessContext processContext) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split("[^\\p{L}]+", -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    processContext.output(word);
                }
            }
        }
    }
}
