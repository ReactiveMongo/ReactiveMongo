package reactivemongo.core;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public final class SSL {
    /**
     * @param sslContext the sslContext the be used
     * @param host the hostname of the server
     * @param port the server port
     */
    public static SSLEngine createEngine(SSLContext sslContext,
                                         String host, int port) {

        SSLEngine engine = sslContext.createSSLEngine(host, port);

        engine.setUseClientMode(true);

        SSLParameters params = engine.getSSLParameters();

        params.setServerNames(java.util.Collections.<SNIServerName>singletonList(new SNIHostName(host)));

        engine.setSSLParameters(params);

        return engine;
    }
}
