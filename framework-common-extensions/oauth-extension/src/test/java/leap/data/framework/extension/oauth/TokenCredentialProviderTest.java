package leap.data.framework.extension.oauth;

import leap.data.framework.extension.oauth.httpclient.HttpClientFactory;
import leap.data.framework.extension.oauth.model.AuthenticationData;
import leap.data.framework.extension.oauth.model.Token;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

import static org.easymock.EasyMock.*;

public class TokenCredentialProviderTest {
    private HttpEntity httpEntity;
    private TokenCredentialProvider tokenCredentialProvider;

    @Before
    public void setup() throws Exception {
        //given:
        HttpClientFactory httpClientFactory = createNiceMock(HttpClientFactory.class);
        CloseableHttpClient httpClient = createNiceMock(CloseableHttpClient.class);
        CloseableHttpResponse httpResponse = createNiceMock(CloseableHttpResponse.class);
        httpEntity = createNiceMock(HttpEntity.class);

        //and:
        expect(httpClientFactory.getCloseableHttpClient()).andReturn(httpClient).anyTimes();
        expect(httpClient.execute(EasyMock.anyObject())).andReturn(httpResponse).anyTimes();
        expect(httpResponse.getEntity()).andReturn(httpEntity).anyTimes();
        replay(httpClientFactory, httpClient, httpResponse);

        //and:
        AuthenticationData authenticationData = new AuthenticationData("testClient","testClient","http://test");
        tokenCredentialProvider = new TokenCredentialProvider(authenticationData, httpClientFactory);
    }

    @Test
    public void testGetTokenReturnsValidToken() throws IOException {
        //given:
        String jsonToken = "{\"access_token\":\"token\",\"expires_in\":\"1000\"}";
        InputStream contentStream = new ByteArrayInputStream(jsonToken.getBytes());

        //and:
        expect(httpEntity.getContent()).andReturn(contentStream);
        replay(httpEntity);

        //when:
        Token token = tokenCredentialProvider.getToken();

        //then:
        assertThat(token).isNotNull();
        assertThat(token.getAccessTokenValue()).isEqualTo("token");
        assertThat(token.isExpired()).isEqualTo(false);
    }

    @Test
    public void testGetNewTokenWhenExpired() throws IOException {
        //given:
        String jsonToken = "{\"access_token\":\"token\",\"expires_in\":\"1\"}";
        InputStream contentStream = new ByteArrayInputStream(jsonToken.getBytes());
        String newjsonToken = "{\"access_token\":\"newToken\",\"expires_in\":\"1\"}";
        InputStream newcontentStream = new ByteArrayInputStream(newjsonToken.getBytes());

        //and:
        expect(httpEntity.getContent()).andReturn(contentStream).andReturn(newcontentStream);
        replay(httpEntity);

        //when:
        Token token = tokenCredentialProvider.getToken();

        //then:
        assertThat(token.isExpired()).isEqualTo(true);

        //when:
        token = tokenCredentialProvider.getToken();

        //then:
        assertThat(token.getAccessTokenValue()).isEqualTo("newToken");
    }
}
