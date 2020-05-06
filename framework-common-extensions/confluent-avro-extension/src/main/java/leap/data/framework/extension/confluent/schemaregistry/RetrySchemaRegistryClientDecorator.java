package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.backoff.ExponentialBackoff;
import leap.data.framework.core.backoff.ExponentialBackoffBuilder;
import org.apache.avro.Schema;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class RetrySchemaRegistryClientDecorator extends DefaultSchemaRegistryClientDecorator {
    private ExponentialBackoffBuilder exponentialBackoffBuilder;
    public RetrySchemaRegistryClientDecorator(SchemaRegistryClient decoratedClient, Map<String, ?> config){
        super(decoratedClient);
        exponentialBackoffBuilder = new ExponentialBackoffBuilder();
        if(config.containsKey("serializer.registry.retry.maxRetries"))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxRetries((Integer)config.get("serializer.registry.retry.maxRetries"));
        if(config.containsKey("serializer.registry.retry.initialBackoff"))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withInitialBackoff((Long)config.get("serializer.registry.retry.initialBackoff"));
        if(config.containsKey("serializer.registry.retry.maxBackoff"))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxBackoff((Long)config.get("serializer.registry.retry.maxBackoff"));
        if(config.containsKey("serializer.registry.retry.exponent"))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withExponent((Integer)config.get("serializer.registry.retry.exponent"));
        if(config.containsKey("serializer.registry.retry.maxCumulativeBackoff"))
            exponentialBackoffBuilder = exponentialBackoffBuilder.withMaxCumulativeBackoff((Integer)config.get("serializer.registry.retry.maxCumulativeBackoff"));
    }

    @FunctionalInterface
    public interface SchemaRegistryClientSupplier<R> {
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
