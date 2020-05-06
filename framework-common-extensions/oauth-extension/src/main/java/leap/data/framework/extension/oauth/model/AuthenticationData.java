package leap.data.framework.extension.oauth.model;

import java.net.URI;
import java.net.URISyntaxException;

public class AuthenticationData {
    private final String clientId;
    private final String clientSecret;
    private final URI tokenURI;
    private final String grantType;

    public AuthenticationData(String clientId, String clientSecret, String tokenURI) throws URISyntaxException {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tokenURI = new URI(tokenURI);
        grantType = "client-credentials";
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public URI getTokenURI() {
        return tokenURI;
    }

    public String getGrantType() {
        return grantType;
    }
}
