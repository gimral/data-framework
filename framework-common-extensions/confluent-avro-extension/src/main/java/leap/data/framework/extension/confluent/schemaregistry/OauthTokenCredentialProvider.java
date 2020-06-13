package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import leap.data.framework.extension.oauth.TokenCredentialProvider;
import leap.data.framework.extension.oauth.model.AuthenticationData;
import org.apache.kafka.common.config.ConfigException;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

//Will be used by configuration
@SuppressWarnings("unused")
public class OauthTokenCredentialProvider implements BearerAuthCredentialProvider {
    public static final String CLIENT_ID = "bearer.auth.clientId";
    public static final String CLIENT_SECRET = "bearer.auth.clientSecret";
    public static final String OAUTH_HOST = "bearer.auth.host";
    public static final String CREDENTIALS_SOURCE = "bearer.auth.credentials.source";

    private TokenCredentialProvider tokenCredentialProvider;

    @Override
    public String alias() {
        return "OAUTH_TOKEN";
    }

    @Override
    public String getBearerToken(URL url) {
        return tokenCredentialProvider.getToken().getAccessTokenValue();
    }

    @Override
    public void configure(Map<String, ?> map) {
        if(!map.containsKey("bearer.auth.clientId") ||
           !map.containsKey("bearer.auth.clientSecret") ||
           !map.containsKey("bearer.auth.host") )   {
            throw new ConfigException(String.format("%s,%s,%s must be provided when %s is set as %s",
                    CLIENT_ID,CLIENT_SECRET,OAUTH_HOST,alias()));
        }
        String clientId = (String) map.get("bearer.auth.clientId");
        String clientSecret = (String) map.get("bearer.auth.clientSecret");
        String ssoTokenUri = (String) map.get("bearer.auth.host");
        String insecureClientStr = (String) map.get(LeapSchemaRegistryClient.INSECURE_CLIENT);
        boolean insecureClient = insecureClientStr != null && insecureClientStr.equals("true");
        try{
            tokenCredentialProvider = new TokenCredentialProvider(new AuthenticationData(clientId, clientSecret, ssoTokenUri), insecureClient);
        } catch (URISyntaxException e) {
            throw new ConfigException("ssoTokenUrl is malformed");
        }
    }
}
