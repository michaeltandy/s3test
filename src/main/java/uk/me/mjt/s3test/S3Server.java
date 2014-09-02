
package uk.me.mjt.s3test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Server {
    public static final int NUMBER_OF_THREADS = 3;

    private static final Pattern BUCKET_PATTERN = Pattern.compile("/([^/]+)/"); // Format is like "/bucketname/"
    private static final Pattern REQUEST_PATH_PATTERN = Pattern.compile("/([^/]+)/(.+)"); // Format is like "/bucketname/asdf.txt"
    private static final Pattern DOUBLE_DOT_PATTERN = Pattern.compile("(.*/)?\\.\\./+\\.\\.(/.*)?");

    // Bucket names must follow DNS rules from http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    private static final Pattern BUCKET_NAME_MUST_MATCH_PATTERN = Pattern.compile("[a-z0-9\\.\\-]{3,63}");
    private static final Pattern BUCKET_NAME_MUST_NOT_MATCH_PATTERN = Pattern.compile("(^[\\.\\-])|"
        + "([\\.\\-]$)|"
        + "([\\.\\-]{2})|"
        + "(^\\d+\\.\\d+\\.\\d+\\.\\d+$)");
    public static final int BASE_PORT_NUMBER = 8000;
    public static final int PORT_NUMBER_RANGE = 1000;
    public static final String PREFIX_QUERY_PARAMETER_NAME = "prefix";

    private final HttpServer httpServer;
    private final HashMap<String,Bucket> buckets = new HashMap<>();
    private ExecutorService executorService = null;
    private InetSocketAddress address;

    public S3Server() throws IOException {
        this(null);
    }

    public S3Server(InetSocketAddress address) throws IOException {
        this.httpServer = HttpServer.create();
        this.address = address;
        createContext();
    }

    public void start() throws IOException {
        executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS, new ThreadFactory() {
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

    private void createContext() {
        // create and register our handler
        httpServer.createContext("/", new HttpHandler() {
            public void handle(HttpExchange exchange) throws IOException {
                System.out.println(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                switch(exchange.getRequestMethod()) {
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

    private void handleGet(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if(path.equals("/")) {
            handleListBuckets(exchange);
            return;
        }

        Matcher bucketPatternMatcher = BUCKET_PATTERN.matcher(path);
        if(bucketPatternMatcher.matches()) {
            String bucketName = bucketPatternMatcher.group(1);
            if(buckets.containsKey(bucketName)) {
                String prefix = getPrefixQueryParam(exchange.getRequestURI());
                handleListObjects(exchange, bucketName, prefix);
                return;
            } else {
                respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
                return;
            }
        }

        Matcher matcher = REQUEST_PATH_PATTERN.matcher(path);
        if (!matcher.matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }
        String bucketName = matcher.group(1);
        String keyName = matcher.group(2);

        if (DOUBLE_DOT_PATTERN.matcher(keyName).matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }

        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            if (bucket.containsKey(keyName)) {
                respondFoundObjectAndClose(exchange, bucket.get(keyName));
            } else {
                respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_KEY);
            }
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private String getPrefixQueryParam(URI requestURI) {
        String query = requestURI.getQuery();
        if(query == null || query.isEmpty()) {
            return "";
        }

        String[] queryParameters = query.split("&");
        for(String queryParameter : queryParameters) {
            String[] queryPair = queryParameter.split("=");
            if(queryPair.length >= 2 && queryPair[0].equals(PREFIX_QUERY_PARAMETER_NAME)) {
                return queryPair[1];
            }
        }
        return "";
    }

    private void handleListBuckets(HttpExchange exchange) throws IOException {
        String response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<ListAllMyBucketsResult>\n" +
            "   <Owner>\n" +
            "       <DisplayName>S3 Test</DisplayName>\n" +
            "   </Owner>\n" +
            "   <Buckets>\n";

        for(String bucketName : buckets.keySet()) {
            response = response +
                "       <Bucket>\n" +
                "           <Name>" + bucketName + "</Name>" +
                "       </Bucket>\n";
        }

        response = response +
            "   </Buckets>\n" +
            "</ListAllMyBucketsResult>";
        respondOkAndClose(exchange, response.getBytes());
    }

    private void handleListObjects(HttpExchange exchange, String bucketName, String prefix) throws IOException {
        String response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<ListBucketResult>\n" +
            "   <Name>" + bucketName + "</Name>\n" +
            "   <Prefix/>\n" +
            "   <Marker/>\n" +
            "   <MaxKeys>1000</MaxKeys>\n" +
            "   <IsTruncated>false</IsTruncated>\n";

        Bucket bucket = buckets.get(bucketName);
        for(String objectName : bucket.keySet()) {
            if(objectNameHasPrefix(prefix, objectName)) {
                StoredObject storedObject = bucket.get(objectName);
                response = response +
                    "       <Contents>\n" +
                    "           <Key>" + objectName + "</Key>" +
                    "           <LastModified>" + "2014-06-30T14:44:48.000Z" + "</LastModified>" +
                    "           <ETag>\"" + storedObject.md5HexString() + "\"</ETag>" +
                    "           <Size>" + storedObject.getContent().length + "</Size>" +
                    "           <Owner>" +
                    "               <ID>1a542c1f154dd21c5baff54212</ID>" +
                    "               <DisplayName>s3-test</DisplayName>" +
                    "           </Owner>" +
                    "           <StorageClass>STANDARD</StorageClass>" +
                    "       </Contents>\n";
            }
        }

        response = response +
            "</ListBucketResult>";
        respondOkAndClose(exchange, response.getBytes());
    }

    private boolean objectNameHasPrefix(String prefix, String objectName) {
        if(prefix.isEmpty()) {
            return true;
        }
        if(objectName.startsWith("/")) {
            objectName = objectName.substring(1);
        }
        return objectName.startsWith(prefix);
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        Matcher bucket = BUCKET_PATTERN.matcher(path);
        if (bucket.matches()) {
            String bucketName = bucket.group(1);
            handlePutBucket(exchange, bucketName);
            return;
        }

        Matcher matcher = REQUEST_PATH_PATTERN.matcher(path);
        if (matcher.matches()) {
            String bucketName = matcher.group(1);
            String keyName = matcher.group(2);
            handlePutObject(exchange, bucketName, keyName);
            return;
        }
        respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
    }

    private void handlePutObject(HttpExchange exchange, String bucketName, String keyName) throws IOException {
        if (DOUBLE_DOT_PATTERN.matcher(keyName).matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }

        byte[] content = readRequestBodyFully(exchange);

        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            StoredObject storedObject = new StoredObject(content);
            bucket.put(keyName, storedObject);
            addHeader(exchange, HttpHeaders.E_TAG, "\"" + storedObject.md5HexString() + "\"");

            respondOkAndClose(exchange);
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private void handlePutBucket(HttpExchange exchange,String bucketName) throws IOException {
        byte[] reqBody = readRequestBodyFully(exchange);

        if (!bucketNameValid(bucketName)) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_BUCKET_NAME);
        } else if (buckets.containsKey(bucketName)) {
            respondErrorAndClose(exchange, ErrorResponse.BUCKET_ALREADY_EXISTS);
        } else {
            System.out.println("Creating bucket " + bucketName + ".");
            buckets.put(bucketName, new Bucket());
            addHeader(exchange, HttpHeaders.LOCATION, "/" + bucketName);
            respondOkAndClose(exchange);
        }
    }

    static boolean bucketNameValid(String bucketName) {
        if (bucketName == null) {
            return false;
        }
        Matcher m1 = BUCKET_NAME_MUST_MATCH_PATTERN.matcher(bucketName);
        Matcher m2 = BUCKET_NAME_MUST_NOT_MATCH_PATTERN.matcher(bucketName);
        return m1.matches() && !m2.find();
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        Matcher bucket = BUCKET_PATTERN.matcher(path);
        if (bucket.matches()) {
            String bucketName = bucket.group(1);
            handleDeleteBucket(exchange,bucketName);
            return;
        }

        Matcher m = REQUEST_PATH_PATTERN.matcher(path);
        if (m.matches()) {
            String bucketName = m.group(1);
            String keyName = m.group(2);
            handleDeleteObject(exchange,bucketName,keyName);
            return;
        }

        respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
    }

    private void handleDeleteObject(HttpExchange exchange,String bucketName,String keyName) throws IOException {
        byte[] content = readRequestBodyFully(exchange);

        if (buckets.containsKey(bucketName)) {
            Bucket b = buckets.get(bucketName);
            b.remove(keyName);
        }
        respondNoContentAndClose(exchange);
    }

    private void handleDeleteBucket(HttpExchange exchange,String bucketName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            if(bucket.isEmpty()) {
                respondErrorAndClose(exchange, ErrorResponse.BUCKET_NOT_EMPTY);
            } else {
                buckets.remove(bucketName);
                respondNoContentAndClose(exchange);
            }
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private void addHeader(HttpExchange exchange, String name, String value) {
        // RFC 2616 says HTTP headers are case-insensitive - but the
        // Amazon S3 client will crash if ETag has a different
        // capitalisation. And this HttpServer normalises the names
        // of headers using "ETag"->"Etag" if you use put, add or
        // set. But not if you use 'putAll' so that's what I use.
        Map<String, List<String>> responseHeaders = Collections.singletonMap(name, Collections.singletonList(value));
        exchange.getResponseHeaders().putAll(responseHeaders);
    }

    private void respondFoundObjectAndClose(HttpExchange exchange, StoredObject obj) throws IOException {
        byte[] response = obj.getContent();

        addHeader(exchange, "ETag","\"" + obj.md5HexString() + "\"");
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "text/plain");
        respondOkAndClose(exchange, response);
    }

    private void respondErrorAndClose(HttpExchange exchange, ErrorResponse errorResponse) throws IOException {
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "application/xml");
        respondAndClose(exchange, errorResponse.getStatusCode(), errorResponse.getAsXml().getBytes());
    }

    private void respondOkAndClose(HttpExchange exchange) throws IOException {
        respondAndClose(exchange, HttpURLConnection.HTTP_OK, new byte[0]);
    }

    private void respondOkAndClose(HttpExchange exchange, byte[] response) throws IOException {
        respondAndClose(exchange, HttpURLConnection.HTTP_OK, response);
    }

    private void respondNoContentAndClose(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
        exchange.close();
    }

    private void respondAndClose(HttpExchange exchange, int httpCode, byte[] response) throws IOException {
        exchange.sendResponseHeaders(httpCode, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private byte[] readRequestBodyFully(HttpExchange exchange) throws IOException {
        // FIXME missing header, non-integer, negative, larger-than-integer, more data than content length, multipart etc.
        String lengthHeader = exchange.getRequestHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);
        if (lengthHeader == null) {
            lengthHeader= "0";
        }
        int contentLength = Integer.parseInt(lengthHeader);
        if (contentLength > 0) {
            byte[] content = new byte[contentLength];
            InputStream is = exchange.getRequestBody();
            int lengthRead = is.read(content);
            is.close();
            content = Arrays.copyOf(content, lengthRead);
            return content;
        } else {
            return new byte[0];
        }
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
