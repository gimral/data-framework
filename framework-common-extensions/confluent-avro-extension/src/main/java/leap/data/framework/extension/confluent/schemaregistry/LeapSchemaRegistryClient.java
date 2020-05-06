package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;

import java.util.List;
import java.util.Map;

public class LeapSchemaRegistryClient extends CachedSchemaRegistryClient {
    public LeapSchemaRegistryClient(RestService restService, int identityMapCapacity, Map<String, ?> configs, Map<String, String> httpHeaders) {
        super(restService, identityMapCapacity, configs, httpHeaders);
    }
    public LeapSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity, Map<String, ?> originals, Map<String, String> httpHeaders) {
        super(baseUrls, identityMapCapacity, originals, httpHeaders);
    }
}
