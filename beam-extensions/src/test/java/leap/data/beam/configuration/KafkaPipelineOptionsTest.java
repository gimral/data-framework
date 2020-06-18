package leap.data.beam.configuration;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaPipelineOptionsTest {
    @Test
    public void testKafkaPipelineOptions(){
        //given:
        String[] args = new String[5];
        args[0] = "--inputTopics={\"topic1\":\"dev-topic1\",\"topic2\":\"dev-topic2\"}";
        PipelineOptionsFactory.register(KafkaPipelineOptions.class);

        //when:
        KafkaPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(KafkaPipelineOptions.class);

        //then:
        assertThat(options.getInputTopics()).hasSize(2);
        assertThat(options.getInputTopics()).containsValues("dev-topic1","dev-topic2");
    }
}
