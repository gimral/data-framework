package leap.data.framework.extension.oauth.httpclient;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

//To make HttpClients static methods testable
public class HttpClientFactory {

    public CloseableHttpClient getCloseableHttpClient() throws Exception {
        return HttpClients.createDefault();
    }
}
