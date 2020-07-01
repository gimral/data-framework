package leap.data.examples.beam.configuration;

import leap.data.beam.configuration.LeapPipelineOptions;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
/**
 * Options supported by {@link WordCount}.
 *
 * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
 * be processed by the command-line parser, and specify default values for them. You can then
 * access the options values in your pipeline code.
 *
 * <p>Inherits standard configuration options.
 */
public interface WordCountOptions extends LeapPipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("src/main/resources/input.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);
}
