package reactivemongo.core;

import javax.net.ssl.SSLParameters;
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

        System.err.println("Java 7 doesn't support SNI");

        return engine;
    }
}
