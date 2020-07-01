package leap.data.examples.beam;

import leap.data.examples.beam.pipeline.WordCountPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);
    public static void main(String[] args) {
        logger.info("WordCount started");
        WordCountPipeline pipeline = new WordCountPipeline();
        pipeline.run(args);
    }
}
