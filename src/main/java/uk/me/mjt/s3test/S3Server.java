package uk.me.mjt.s3test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import uk.me.mjt.s3test.xml.ErrorResponseXmlDocument;
import uk.me.mjt.s3test.xml.ListBucketsXmlDocument;
import uk.me.mjt.s3test.xml.ListObjectsXmlDocument;
import uk.me.mjt.s3test.xml.XmlDocument;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Server extends Server {

    private static final int NUMBER_OF_THREADS = 3;

    private static final Pattern BUCKET_PATTERN = Pattern.compile("/([^/]+)/"); // Format is like "/bucketname/"
    private static final Pattern REQUEST_PATH_PATTERN = Pattern.compile("/([^/]+)/(.+)"); // Format is like "/bucketname/asdf.txt"
    private static final Pattern DOUBLE_DOT_PATTERN = Pattern.compile("(.*/)?\\.\\./+\\.\\.(/.*)?");

    // Bucket names must follow DNS rules from http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    private static final Pattern BUCKET_NAME_MUST_MATCH_PATTERN = Pattern.compile("[a-z0-9\\.\\-]{3,63}");
    private static final Pattern BUCKET_NAME_MUST_NOT_MATCH_PATTERN = Pattern.compile("(^[\\.\\-])|"
        + "([\\.\\-]$)|"
        + "([\\.\\-]{2})|"
        + "(^\\d+\\.\\d+\\.\\d+\\.\\d+$)");

    public static final String PREFIX_QUERY_PARAMETER = "prefix";
    public static final String S3_TEST_OWNER_ID = "7aab9dc7212a1061887ecb";
    public static final String S3_TEST_OWNER_DISPLAY_NAME = "S3 Test";

    private final Map<String, Bucket> buckets = new HashMap<>();
    private final boolean isHttps;

    private S3Server(InetSocketAddress address) throws IOException {
        super(address, NUMBER_OF_THREADS, HttpServer.create());
        this.isHttps = false;
    }

    private S3Server(InetSocketAddress address, InputStream keystoreInputSteam, char[] password) throws IOException {
        super(address, NUMBER_OF_THREADS, initHttpsServer(keystoreInputSteam, password));
        this.isHttps = true;
    }

    public static S3Server createHttpServer() throws IOException {
        return new S3Server(null);
    }

    public static S3Server createHttpServer(InetSocketAddress address) throws IOException {
        return new S3Server(address);
    }

    public static S3Server createHttpsServer(InputStream keystoreInputSteam, char[] password) throws IOException {
        return new S3Server(null, keystoreInputSteam, password);
    }

    public static S3Server createHttpsServer(
        InetSocketAddress address,
        InputStream keystoreInputSteam, char[]
        password) throws IOException {
        return new S3Server(address, keystoreInputSteam, password);
    }

    private static HttpServer initHttpsServer(InputStream keystoreInputSteam, char[] password) throws IOException {
            try {
                KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(keystoreInputSteam, password);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                kmf.init(ks, password);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ks);

                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                HttpsServer httpsServer = HttpsServer.create();
                httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                return httpsServer;
            } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | UnrecoverableKeyException
                | KeyManagementException e) {
                throw new RuntimeException(e);
            }
    }


    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/")) {
            handleListBuckets(exchange);
            return;
        }

        Matcher bucketPatternMatcher = BUCKET_PATTERN.matcher(path);
        if (bucketPatternMatcher.matches()) {
            String bucketName = bucketPatternMatcher.group(1);
            handleListObjects(exchange, bucketName);
        }

        Matcher requestPathPatternMatcher = REQUEST_PATH_PATTERN.matcher(path);
        if (!requestPathPatternMatcher.matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }
        String bucketName = requestPathPatternMatcher.group(1);
        String keyName = requestPathPatternMatcher.group(2);

        if (DOUBLE_DOT_PATTERN.matcher(keyName).matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }

        handleGetObject(exchange, bucketName, keyName);
    }

    @Override
    protected void handlePut(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        Matcher bucketPatternMatcher = BUCKET_PATTERN.matcher(path);
        if (bucketPatternMatcher.matches()) {
            String bucketName = bucketPatternMatcher.group(1);
            handlePutBucket(exchange, bucketName);
            return;
        }

        Matcher requestPathPatternMatcher = REQUEST_PATH_PATTERN.matcher(path);
        if (requestPathPatternMatcher.matches()) {
            String bucketName = requestPathPatternMatcher.group(1);
            String keyName = requestPathPatternMatcher.group(2);
            handlePutObject(exchange, bucketName, keyName);
            return;
        }
        respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
    }

    @Override
    protected void handleDelete(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        Matcher bucketPatternMatcher = BUCKET_PATTERN.matcher(path);
        if (bucketPatternMatcher.matches()) {
            String bucketName = bucketPatternMatcher.group(1);
            handleDeleteBucket(exchange,bucketName);
            return;
        }

        Matcher requestPathPatternMatcher = REQUEST_PATH_PATTERN.matcher(path);
        if (requestPathPatternMatcher.matches()) {
            String bucketName = requestPathPatternMatcher.group(1);
            String keyName = requestPathPatternMatcher.group(2);
            handleDeleteObject(exchange, bucketName, keyName);
            return;
        }

        respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
    }

    @Override
    public String getAddress() {
            String protocol;
            if (isHttps) {
                protocol = "https://";
            } else {
                protocol = "http://";
            }
            return protocol + hostName.getHostName() + ":" + hostName.getPort();
    }

    private void handleGetObject(HttpExchange exchange, String bucketName, String keyName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            if (bucket.containsKey(keyName)) {
                respondGetObjectAndClose(exchange, bucket.get(keyName));
            } else {
                respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_KEY);
            }
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private void handleListObjects(HttpExchange exchange, String bucketName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            String prefix = getQueryParamValue(exchange.getRequestURI(), PREFIX_QUERY_PARAMETER);
            respondListObjectsAndClose(exchange, bucketName, prefix);
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }


    private void handleListBuckets(HttpExchange exchange) throws IOException {
        respondWithXmlDocumentAndClose(
            exchange,
            HttpURLConnection.HTTP_OK,
            new ListBucketsXmlDocument(
                buckets,
                S3_TEST_OWNER_ID,
                S3_TEST_OWNER_DISPLAY_NAME
            )
        );
    }

    private void handlePutObject(HttpExchange exchange, String bucketName, String keyName) throws IOException {
        if (DOUBLE_DOT_PATTERN.matcher(keyName).matches()) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_URI);
            return;
        }

        byte[] content = readRequestBodyFully(exchange);

        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            StoredObject storedObject = new StoredObject(keyName, content);
            bucket.put(keyName, storedObject);
            addHeader(exchange, HttpHeaders.E_TAG, "\"" + storedObject.md5HexString() + "\"");
            respondAndClose(exchange, HttpURLConnection.HTTP_OK);
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private void handlePutBucket(HttpExchange exchange, String bucketName) throws IOException {
        readRequestBodyFully(exchange);

        if (!bucketNameValid(bucketName)) {
            respondErrorAndClose(exchange, ErrorResponse.INVALID_BUCKET_NAME);
        } else if (buckets.containsKey(bucketName)) {
            respondErrorAndClose(exchange, ErrorResponse.BUCKET_ALREADY_EXISTS);
        } else {
            System.out.println("Creating bucket " + bucketName + ".");
            buckets.put(bucketName, new Bucket(bucketName));
            addHeader(exchange, HttpHeaders.LOCATION, "/" + bucketName);
            respondAndClose(exchange, HttpURLConnection.HTTP_OK);
        }
    }

    private void handleDeleteObject(HttpExchange exchange, String bucketName, String keyName) throws IOException {
        readRequestBodyFully(exchange);

        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            bucket.remove(keyName);
        }
        respondAndClose(exchange, HttpURLConnection.HTTP_NO_CONTENT);
    }

    private void handleDeleteBucket(HttpExchange exchange, String bucketName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            if (bucket.isEmpty()) {
                respondErrorAndClose(exchange, ErrorResponse.BUCKET_NOT_EMPTY);
            } else {
                buckets.remove(bucketName);
                respondAndClose(exchange, HttpURLConnection.HTTP_NO_CONTENT);
            }
        } else {
            respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
        }
    }

    private boolean bucketNameValid(String bucketName) {
        return bucketName != null
            && BUCKET_NAME_MUST_MATCH_PATTERN.matcher(bucketName).matches()
            && !BUCKET_NAME_MUST_NOT_MATCH_PATTERN.matcher(bucketName).find();
    }

    private void respondListObjectsAndClose(HttpExchange exchange, String bucketName, String prefix) throws IOException {
        respondWithXmlDocumentAndClose(
            exchange,
            HttpURLConnection.HTTP_OK,
            new ListObjectsXmlDocument(
                buckets.get(bucketName),
                prefix,
                S3_TEST_OWNER_ID,
                S3_TEST_OWNER_DISPLAY_NAME
            )
        );
    }

    private void respondGetObjectAndClose(HttpExchange exchange, StoredObject storedObject) throws IOException {
        byte[] response = storedObject.getContent();

        addHeader(exchange, "ETag", "\"" + storedObject.md5HexString() + "\"");
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "text/plain");
        respondAndClose(exchange, HttpURLConnection.HTTP_OK, response);
    }

    private void respondWithXmlDocumentAndClose(HttpExchange exchange, int httpCode, XmlDocument xmlDocument) throws IOException {
        xmlDocument.build();
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "application/xml");
        respondAndClose(exchange, httpCode, xmlDocument.toUtf8Bytes());
    }

    private void respondErrorAndClose(HttpExchange exchange, ErrorResponse errorResponse) throws IOException {
        respondWithXmlDocumentAndClose(
            exchange,
            errorResponse.getStatusCode(),
            new ErrorResponseXmlDocument(
                errorResponse
            )
        );
    }
}
