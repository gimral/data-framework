package leap.data.framework.extension.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import leap.data.framework.extension.oauth.httpclient.HttpClientFactory;
import leap.data.framework.extension.oauth.httpclient.InsecureHttpClientFactory;
import leap.data.framework.extension.oauth.model.AuthenticationData;
import leap.data.framework.extension.oauth.model.Token;
import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TokenCredentialProvider {
    private static final Logger logger = LoggerFactory.getLogger(TokenCredentialProvider.class);
    private static final String GRANT_TYPE = "grant_type";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String CLIENT_ID = "client_id";


    private final AuthenticationData authenticationData;
    private final ObjectMapper objectMapper;
    private final HttpClientFactory httpClientFactory;

    private Token token;

    public TokenCredentialProvider(AuthenticationData authenticationData) {
        this(authenticationData, new HttpClientFactory());
    }

    public TokenCredentialProvider(AuthenticationData authenticationData, boolean insecureClient) {
        this(authenticationData, insecureClient ? new InsecureHttpClientFactory() : new HttpClientFactory());
    }

    public TokenCredentialProvider(AuthenticationData authenticationData, HttpClientFactory httpClientFactory) {
        this.authenticationData = authenticationData;
        this.httpClientFactory = httpClientFactory;
        this.objectMapper = new ObjectMapper();
    }

    public Token getToken() {
        //If there is no access token or if it has expired get new token from sso
        if(token == null || token.isExpired()){
            try {
                token = getNewToken();
            } catch (Exception e) {
                //TODO: Implement Retry
                e.printStackTrace();
            }
        }
        return token;
    }

    private HttpPost buildHttpPost(){
        HttpPost tokenPost = new HttpPost(authenticationData.getTokenURI());
        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair(CLIENT_ID, authenticationData.getClientId()));
        urlParameters.add(new BasicNameValuePair(CLIENT_SECRET, authenticationData.getClientSecret()));
        urlParameters.add(new BasicNameValuePair(GRANT_TYPE, authenticationData.getGrantType()));

        try {
            tokenPost.setEntity(new UrlEncodedFormEntity(urlParameters));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return tokenPost;
    }

    private synchronized Token getNewToken() throws Exception {
        if(token == null || token.isExpired()) {
            HttpPost tokenPost = buildHttpPost();

            long currentTimeMillis = System.currentTimeMillis();
            try (CloseableHttpClient httpClient = httpClientFactory.getCloseableHttpClient();
                 CloseableHttpResponse response = httpClient.execute(tokenPost)) {
                //TODO: Check status code to determine successful response
                HttpEntity responseEntity = response.getEntity();
                Header encodingHeader = responseEntity.getContentEncoding();
                Charset encoding = encodingHeader == null ? StandardCharsets.UTF_8 :
                        Charsets.toCharset(encodingHeader.getValue());

                String json = EntityUtils.toString(responseEntity, encoding);
                token = objectMapper.readValue(json, Token.class);
                token.setExpireTimeMillis(currentTimeMillis);
                return token;
            } catch (Exception e) {
                logger.error("Could not get new token",e);
                throw e;
            }
        }
        else{
            return token;
        }
    }
}
