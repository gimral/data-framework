package leap.data.beam.transforms;

import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.io.ProducerFactoryFn;
import leap.data.beam.transforms.dlq.DeadLetterHeader;
import leap.data.beam.transforms.dlq.DeadLetterWriteTransform;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeadLetterQueueTest {
    private static final KafkaPipelineOptions options;

    static {
        options = PipelineOptionsFactory.fromArgs("--kafkaBootstrapServers=none",
                "--kafkaSchemaRegistryUrl=mock://",
                "--kafkaSecurityProtocol=PLAINTEXT",
                "--kafkaSaslMechanism=")
                .as(KafkaPipelineOptions.class);
    }

    @Rule
    public TestPipeline p = TestPipeline.fromOptions(options);
    @Test
    public void test(){
        //int numElements = 1000;
        try (ProducerFactoryFn.MockProducerWrapper<KV<String,String>> producerWrapper = new ProducerFactoryFn.MockProducerWrapper<>()) {
            ProducerFactoryFn.ProducerSendCompletionThread<KV<String,String>> completionThread =
                    new ProducerFactoryFn.ProducerSendCompletionThread<>(producerWrapper.mockProducer).start();
            String topic = "dlq-test";

            TestStream<KV<String,String>> inputStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(),StringUtf8Coder.of()))
                    .addElements(KV.of("0","0"))
                    .addElements(KV.of("1","1"))
                    .addElements(KV.of("2","2"))
                    .addElements(KV.of("3","3"))
                    .addElements(KV.of("4","4"))
                    .advanceWatermarkToInfinity();

            p.apply("Input Stream",inputStream)
                    .apply(DeadLetterWriteTransform.<String,String>to(topic)
                    .withHeaderFn((ProcessFunction<KV<String, String>, DeadLetterHeader>) input -> {
                        DeadLetterHeader header = new DeadLetterHeader();
                        header.setErrorReason("Test");
                        header.setErrorDescription("Test");
                        return header;
                    })
                    .withProducerFactoryFn(new ProducerFactoryFn<>(producerWrapper.producerKey)));
            p.run();
            completionThread.shutdown();
            verifyProducerRecords(producerWrapper.mockProducer, topic, 5, true, false);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static <K,V> void verifyProducerRecords(
            MockProducer<K, V> mockProducer,
            String topic,
            int numElements,
            boolean keyIsAbsent,
            boolean verifyTimestamp) {

        // verify that appropriate messages are written to kafka
        List<ProducerRecord<K, V>> sent = mockProducer.history();

        // sort by values
        //sent.sort(Comparator.comparing(ProducerRecord::key));

        for (Integer i = 0; i < numElements; i++) {
            ProducerRecord<K, V> record = sent.get(i);
            assertEquals(topic, record.topic());
            if (!keyIsAbsent) {
                //noinspection AssertEqualsBetweenInconvertibleTypes
                assertEquals(i.toString(), record.key());
            }
            assertEquals(i.toString(), (record.value()));
            if (verifyTimestamp) {
                assertEquals(i.intValue(), record.timestamp().intValue());
            }
        }
    }
}
