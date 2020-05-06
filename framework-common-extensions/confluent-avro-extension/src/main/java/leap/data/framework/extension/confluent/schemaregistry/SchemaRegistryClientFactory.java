package leap.data.framework.extension.confluent.schemaregistry;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SchemaRegistryClientFactory {
    public SchemaRegistryClient getSchemaRegistryClient(LeapSerializerConfig config){
        KafkaAvroSerializerConfig serializerConfig = new KafkaAvroSerializerConfig(config.getProps());
        List<String> urls = serializerConfig.getSchemaRegistryUrls();
        int maxSchemaObject = serializerConfig.getMaxSchemasPerSubject();
        Map<String, Object> originals = serializerConfig.originalsWithPrefix("");
        String mockScope = validateAndMaybeGetMockScope(urls);
        //TODO: Can send a rest client
        SchemaRegistryClient schemaRegistryClient = (mockScope != null) ? MockSchemaRegistry.getClientForScope(mockScope)
                : new LeapSchemaRegistryClient(urls, maxSchemaObject, originals, serializerConfig.requestHeaders());
        //if(config.)
        schemaRegistryClient = new RetrySchemaRegistryClientDecorator(schemaRegistryClient,originals);
        return schemaRegistryClient;
    }

    public SchemaRegistryClient getSchemaRegistryClient(RestService restService, int identityMapCapacity, Map<String, ?> config, Map<String, String> httpHeaders) {
        SchemaRegistryClient schemaRegistryClient = new LeapSchemaRegistryClient(restService, identityMapCapacity, config, httpHeaders);
        schemaRegistryClient = new RetrySchemaRegistryClientDecorator(schemaRegistryClient,config);
        return schemaRegistryClient;
    }


    private static String validateAndMaybeGetMockScope(List<String> urls) {
        List<String> mockScopes = new LinkedList();
        Iterator var2 = urls.iterator();

        while(var2.hasNext()) {
            String url = (String)var2.next();
            if (url.startsWith("mock://")) {
                mockScopes.add(url.substring("mock://".length()));
            }
        }

        if (mockScopes.isEmpty()) {
            return null;
        } else if (mockScopes.size() > 1) {
            throw new ConfigException("Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls);
        } else if (urls.size() > mockScopes.size()) {
            throw new ConfigException("Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls);
        } else {
            return mockScopes.get(0);
        }
    }
}
