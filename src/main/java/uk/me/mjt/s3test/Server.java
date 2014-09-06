package uk.me.mjt.s3test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public abstract class Server {

    public static final int BASE_PORT_NUMBER = 8000;
    public static final int PORT_NUMBER_RANGE = 1000;

    private InetSocketAddress address;
    private final int numberOfThreads;

    private final HttpServer httpServer;
    private ExecutorService executorService = null;

    public Server(InetSocketAddress address,
                  int numberOfThreads) throws IOException {
        this.address = address;
        this.numberOfThreads = numberOfThreads;
        this.httpServer = HttpServer.create();
        createContextAndRegisterHandler();
    }

    protected abstract void handleGet(HttpExchange httpExchange) throws IOException;

    protected abstract void handlePut(HttpExchange httpExchange) throws IOException;

    protected abstract void handleDelete(HttpExchange httpExchange) throws IOException;

    public void start() throws IOException {
        executorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactory() {
            int thisServerThreadCount = 0;

            public Thread newThread(Runnable r) {
                return new Thread(r, "S3Server thread " + (++thisServerThreadCount));
            }
        });
        httpServer.setExecutor(executorService);

        if (address != null) {
            httpServer.bind(address, 0);
        } else {
            bindToRandomPort();
        }
        httpServer.start();
    }

    public void stop() {
        httpServer.stop(1);
        executorService.shutdown();
        executorService = null;
    }

    public String getAddress() {
        return "http://" + address.getHostName() + ":" + address.getPort();
    }

    public String getQueryParamValue(URI requestURI, String name) {
        String query = requestURI.getQuery();
        if (query == null || query.isEmpty()) {
            return "";
        }

        String[] queryParameters = query.split("&");
        for (String queryParameter : queryParameters) {
            String[] queryPair = queryParameter.split("=");
            if (queryPair.length >= 2 && queryPair[0].equals(name)) {
                return queryPair[1];
            }
        }
        return "";
    }

    protected byte[] readRequestBodyFully(HttpExchange exchange) throws IOException {
        // FIXME missing header, non-integer, negative, larger-than-integer, more data than content length, multipart etc.
        String lengthHeader = exchange.getRequestHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);
        if (lengthHeader == null) {
            lengthHeader= "0";
        }
        int contentLength = Integer.parseInt(lengthHeader);
        if (contentLength > 0) {
            byte[] content = new byte[contentLength];
            InputStream inputStream = exchange.getRequestBody();
            int lengthRead = inputStream.read(content);
            inputStream.close();
            content = Arrays.copyOf(content, lengthRead);
            return content;
        } else {
            return new byte[0];
        }
    }

    protected void addHeader(HttpExchange exchange, String name, String value) {
        // RFC 2616 says HTTP headers are case-insensitive - but the
        // Amazon S3 client will crash if ETag has a different
        // capitalisation. And this HttpServer normalises the names
        // of headers using "ETag"->"Etag" if you use put, add or
        // set. But not if you use 'putAll' so that's what I use.
        Map<String, List<String>> responseHeaders = Collections.singletonMap(name, Collections.singletonList(value));
        exchange.getResponseHeaders().putAll(responseHeaders);
    }

    protected void respondAndClose(HttpExchange exchange, int httpCode) throws IOException {
        respondAndClose(exchange, httpCode, new byte[0]);
    }

    protected void respondAndClose(HttpExchange exchange, int httpCode, byte[] response) throws IOException {
        int contentLength = getContentLength(httpCode, response);
        exchange.sendResponseHeaders(httpCode, contentLength);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private int getContentLength(int httpCode, byte[] response) {
        if (httpCode == HttpURLConnection.HTTP_NO_CONTENT) {
            return -1;
        } else {
        return response.length;
        }
    }

    private void createContextAndRegisterHandler() {
        httpServer.createContext("/", new HttpHandler() {
            public void handle(HttpExchange exchange) throws IOException {
                System.out.println(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                switch (exchange.getRequestMethod()) {
                    case HttpMethods.GET:
                        handleGet(exchange);
                        break;
                    case HttpMethods.PUT:
                        handlePut(exchange);
                        break;
                    case HttpMethods.DELETE:
                        handleDelete(exchange);
                        break;
                    default:
                        System.out.println("Don't know how to " + exchange.getRequestMethod());
                        throw new UnsupportedOperationException("Don't know how to " + exchange.getRequestMethod());
                }
            }
        });
    }

    private void bindToRandomPort() throws IOException {
        Random random = new Random();
        for (int i = 0 ; i < 20 ; i++) {
            if (attemptToBind(BASE_PORT_NUMBER + random.nextInt(PORT_NUMBER_RANGE))) {
                return;
            }
        }
        throw new IOException("Made several attempts to bind to a randomly "
                + "chosen port and failed each time. Weird.");
    }

    private boolean attemptToBind(int port) throws IOException {
        try {
            InetSocketAddress binding = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
            httpServer.bind(binding, 0);
            address = binding;
            return true;
        } catch (BindException e) {
            return false;
        }
    }
}
