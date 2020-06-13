package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLSocketFactory;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

public class LeapSchemaRegistryClient extends CachedSchemaRegistryClient {
    public static final String INSECURE_CLIENT = "registry.client.insecure";
    public LeapSchemaRegistryClient(RestService restService, int identityMapCapacity, Map<String, ?> configs, Map<String, String> httpHeaders) {
        super(restService, identityMapCapacity, configs, httpHeaders);
        String bearerCredentialsSource = (String)configs.get(OauthTokenCredentialProvider.CREDENTIALS_SOURCE);
        if(bearerCredentialsSource != null && !bearerCredentialsSource.equals("")){
            BearerAuthCredentialProvider bearerAuthCredentialProvider = new OauthTokenCredentialProvider();
            bearerAuthCredentialProvider.configure(configs);
            restService.setBearerAuthCredentialProvider(bearerAuthCredentialProvider);
        }
        String insecureClientStr = (String)configs.get(LeapSchemaRegistryClient.INSECURE_CLIENT);
        boolean insecureClient = insecureClientStr != null && insecureClientStr.equals("true");
        if(insecureClient)
            restService.setSslSocketFactory(getInsecureSSLSocketFactory());

    }
    public LeapSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity, Map<String, ?> originals, Map<String, String> httpHeaders) {
        super(baseUrls, identityMapCapacity, originals, httpHeaders);
    }

    private SSLSocketFactory getInsecureSSLSocketFactory(){
        try{
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null,((x509Certificates, s) -> true));
            return builder.build().getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }
}
