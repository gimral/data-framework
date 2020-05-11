package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.backoff.ExponentialBackoff;
import leap.data.framework.core.backoff.ExponentialBackoffBuilder;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import org.apache.avro.Schema;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class RetrySchemaRegistryClientDecorator extends DefaultSchemaRegistryClientDecorator {
    private static final String SERIALIZER_REGISTRY_RETRY_MAXRETRIES = "serializer.registry.retry.maxRetries";
    private static final String SERIALIZER_REGISTRY_RETRY_INITIALBACKOFF = "serializer.registry.retry.initialBackoff";
    private static final String SERIALIZER_REGISTRY_RETRY_MAXBACKOFF = "serializer.registry.retry.maxBackoff";
    private static final String SERIALIZER_REGISTRY_RETRY_EXPONENT = "serializer.registry.retry.exponent";
    private static final String SERIALIZER_REGISTRY_RETRY_MAXCUMULATIVEBACKOFF = "serializer.registry.retry.maxCumulativeBackoff";
    private ExponentialBackoffBuilder exponentialBackoffBuilder;
    public RetrySchemaRegistryClientDecorator(SchemaRegistryClient decoratedClient, LeapSerializerConfig config){
        super(decoratedClient);
        Map<?, ?> props = config.getProps();
        exponentialBackoffBuilder = new ExponentialBackoffBuilder();
        if(props.containsKey(SERIALIZER_REGISTRY_RETRY_MAXRETRIES))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxRetries((Integer)props.get(SERIALIZER_REGISTRY_RETRY_MAXRETRIES));
        if(props.containsKey(SERIALIZER_REGISTRY_RETRY_INITIALBACKOFF))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withInitialBackoff((Long)props.get(SERIALIZER_REGISTRY_RETRY_INITIALBACKOFF));
        if(props.containsKey(SERIALIZER_REGISTRY_RETRY_MAXBACKOFF))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxBackoff((Long)props.get(SERIALIZER_REGISTRY_RETRY_MAXBACKOFF));
        if(props.containsKey(SERIALIZER_REGISTRY_RETRY_EXPONENT))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withExponent((Integer)props.get(SERIALIZER_REGISTRY_RETRY_EXPONENT));
        if(props.containsKey(SERIALIZER_REGISTRY_RETRY_MAXCUMULATIVEBACKOFF))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxCumulativeBackoff((Integer)props.get(SERIALIZER_REGISTRY_RETRY_MAXCUMULATIVEBACKOFF));
    }

    @FunctionalInterface
    private interface SchemaRegistryClientSupplier<R> {
        R get() throws IOException, RestClientException;
    }

    private <T> T retry(SchemaRegistryClientSupplier<T> method, ExponentialBackoff exponentialBackoff) throws IOException, RestClientException {
        try{
            return method.get();
        }
        catch (IOException ex){
            Duration nextBackoffTime = exponentialBackoff.nextWait();
            if(nextBackoffTime.toMillis() == 0)
                throw ex;
            try {
                Thread.sleep(nextBackoffTime.toMillis());
            } catch (InterruptedException e) {
                //
            }
            return retry(method,exponentialBackoff);
        }

    }

    @Override
    public synchronized int getId(String subject, Schema schema) throws IOException, RestClientException {
        return retry(() -> super.getId(subject, schema),exponentialBackoffBuilder.build());
    }

    @Override
    public synchronized Schema getById(int id) throws IOException, RestClientException {
        return retry(() -> super.getById(id),exponentialBackoffBuilder.build());
    }

    @Override
    public synchronized int register(String subject, Schema schema) throws IOException, RestClientException {
        return retry(() -> super.register(subject, schema),exponentialBackoffBuilder.build());
    }

    @Override
    public synchronized int getVersion(String subject, Schema schema) throws IOException, RestClientException {
        return retry(() -> super.getVersion(subject, schema),exponentialBackoffBuilder.build());
    }
}
