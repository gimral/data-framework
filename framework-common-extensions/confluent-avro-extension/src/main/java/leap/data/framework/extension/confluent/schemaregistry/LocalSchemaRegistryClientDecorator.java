package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class LocalSchemaRegistryClientDecorator extends DefaultSchemaRegistryClientDecorator{
    private final Map<String, Schema> schemaCache;

    public LocalSchemaRegistryClientDecorator(SchemaRegistryClient decoratedClient) {
        super(decoratedClient);
        schemaCache = new HashMap<>();
        loadLocalSchema();
    }

    private synchronized void loadLocalSchema() {
        try{
            ClassLoader loader = Thread.currentThread().getContextClassLoader();

            InputStream inputStream = loader.getResourceAsStream("avro/avro.properties");
            InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);

            for (String line; (line = reader.readLine()) != null;) {
                try {
                    InputStream schemainputStream = loader.getResourceAsStream("avro/" + line);
                    Schema schema = new Schema.Parser().parse(schemainputStream);
                    schemaCache.put(line.split(".")[0], schema);
                }
                catch (Exception ex){
                    //TODO: Log exception and continue
                }
            }
        }
        catch (Exception ex){
            //TODO: Log exception and continue
        }
    }

}
