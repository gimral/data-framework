package leap.data.beam.io;

import leap.data.beam.serializer.KafkaRecordSerializer;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ProducerFactoryFn<V>
        implements SerializableFunction<Map<String, Object>, Producer<String, V>> {

    public static final ConcurrentMap<String, MockProducer<String, ?>> MOCK_PRODUCER_MAP =
            new ConcurrentHashMap<>();

    final String producerKey;

    ProducerFactoryFn(String producerKey) {
        this.producerKey = producerKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Producer<String, V> apply(Map<String, Object> config) {

        // Make sure the config is correctly set up for serializers.
        Utils.newInstance(
                (Class<? extends Serializer<?>>)
                        ((Class<?>) config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                                .asSubclass(Serializer.class))
                .configure(config, true);

        Utils.newInstance(
                (Class<? extends Serializer<?>>)
                        ((Class<?>) config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                                .asSubclass(Serializer.class))
                .configure(config, false);

        // Returning same producer in each instance in a pipeline seems to work fine currently.
        // If DirectRunner creates multiple DoFn instances for sinks, we might need to handle
        // it appropriately. I.e. allow multiple producers for each producerKey and concatenate
        // all the messages written to each producer for verification after the pipeline finishes.

        return (Producer<String, V>) MOCK_PRODUCER_MAP.get(producerKey);
    }

    @SuppressWarnings("BusyWait")
    public static class MockProducerWrapper<V> implements AutoCloseable {



        final String producerKey;
        final MockProducer<String, V> mockProducer;

        // MockProducer has "closed" method starting version 0.11.
        private static Method closedMethod;

        static {
            try {
                closedMethod = MockProducer.class.getMethod("closed");
            } catch (NoSuchMethodException e) {
                closedMethod = null;
            }
        }

        public MockProducerWrapper() {
            producerKey = String.valueOf(ThreadLocalRandom.current().nextLong());
            mockProducer =
                    new MockProducer<String, V>(
                            false, // disable synchronous completion of send. see ProducerSendCompletionThread
                            // below.
                            new StringSerializer(),
                            new KafkaRecordSerializer<>()) {

                        // override flush() so that it does not complete all the waiting sends, giving a chance
                        // to
                        // ProducerCompletionThread to inject errors.

                        @Override
                        public synchronized void flush() {
                            while (completeNext()) {
                                // there are some uncompleted records. let the completion thread handle them.
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    // ok to retry.
                                }
                            }
                        }
                    };

            // Add the producer to the global map so that producer factory function can access it.
            assertNull(MOCK_PRODUCER_MAP.putIfAbsent(producerKey, mockProducer));
        }

        @Override
        public void close() {
            MOCK_PRODUCER_MAP.remove(producerKey);
            try {
                if (closedMethod == null || !((Boolean) closedMethod.invoke(mockProducer))) {
                    mockProducer.close();
                }
            } catch (Exception e) { // Not expected.
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * We start MockProducer with auto-completion disabled. That implies a record is not marked sent
     * until #completeNext() is called on it. This class starts a thread to asynchronously 'complete'
     * the the sends. During completion, we can also make those requests fail. This error injection is
     * used in one of the tests.
     */
    @SuppressWarnings("BusyWait")
    public static class ProducerSendCompletionThread<V> {

        private final MockProducer<String, V> mockProducer;
        private final int maxErrors;
        private final int errorFrequency;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final ExecutorService injectorThread;
        private int numCompletions = 0;

        ProducerSendCompletionThread(MockProducer<String, V> mockProducer) {
            // complete everything successfully
            this(mockProducer, 0, 0);
        }

        ProducerSendCompletionThread(
                MockProducer<String, V> mockProducer, int maxErrors, int errorFrequency) {
            this.mockProducer = mockProducer;
            this.maxErrors = maxErrors;
            this.errorFrequency = errorFrequency;
            injectorThread = Executors.newSingleThreadExecutor();
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        ProducerSendCompletionThread<V> start() {
            injectorThread.submit(
                    () -> {
                        int errorsInjected = 0;

                        while (!done.get()) {
                            boolean successful;

                            if (errorsInjected < maxErrors && ((numCompletions + 1) % errorFrequency) == 0) {
                                successful =
                                        mockProducer.errorNext(
                                                new InjectedErrorException("Injected Error #" + (errorsInjected + 1)));

                                if (successful) {
                                    errorsInjected++;
                                }
                            } else {
                                successful = mockProducer.completeNext();
                            }

                            if (successful) {
                                numCompletions++;
                            } else {
                                // wait a bit since there are no unsent records
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                    // ok to retry.
                                }
                            }
                        }
                    });

            return this;
        }

        void shutdown() {
            done.set(true);
            injectorThread.shutdown();
            try {
                assertTrue(injectorThread.awaitTermination(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static class InjectedErrorException extends RuntimeException {
        InjectedErrorException(String message) {
            super(message);
        }
    }

}
