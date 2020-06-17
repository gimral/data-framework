package leap.data.framework.extension.oauth.httpclient;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

//To make HttpClients static methods testable
public class InsecureHttpClientFactory extends HttpClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(InsecureHttpClientFactory.class);

    public CloseableHttpClient getCloseableHttpClient() throws Exception {
        try {
            SSLConnectionSocketFactory sslConnectionSocketFactory = getInsecureSSLConnectionSocketFacotry();
            return HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            logger.error("Error when creating insecure client", e);
            throw e;
        }
    }

    private SSLConnectionSocketFactory getInsecureSSLConnectionSocketFacotry() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        SSLContextBuilder builder = new SSLContextBuilder();
        builder.loadTrustMaterial(null, (x509Certificates, authType) -> true);
        return new SSLConnectionSocketFactory(builder.build(), new NoopHostnameVerifier());

    }
}
