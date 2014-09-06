package uk.me.mjt.s3test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Random;
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
