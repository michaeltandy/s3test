
package uk.me.mjt.s3test;

import com.sun.net.httpserver.HttpExchange;
import uk.me.mjt.s3test.xml.ErrorResponseXmlDocument;
import uk.me.mjt.s3test.xml.ListBucketsXmlDocument;
import uk.me.mjt.s3test.xml.ListObjectsXmlDocument;
import uk.me.mjt.s3test.xml.XmlDocument;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Server extends Server {
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

    public static final String PREFIX_QUERY_PARAMETER = "prefix";
    public static final String S3_TEST_OWNER_ID = "7aab9dc7212a1061887ecb";
    public static final String S3_TEST_OWNER_DISPLAY_NAME = "S3 Test";

    private final HashMap<String,Bucket> buckets = new HashMap<>();

    public S3Server() throws IOException {
        this(null);
    }

    public S3Server(InetSocketAddress address) throws IOException {
        super(address, NUMBER_OF_THREADS);
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
            if (buckets.containsKey(bucketName)) {
                String prefix = getQueryParamValue(exchange.getRequestURI(), PREFIX_QUERY_PARAMETER);
                handleListObjects(exchange, bucketName, prefix);
                return;
            } else {
                respondErrorAndClose(exchange, ErrorResponse.NO_SUCH_BUCKET);
                return;
            }
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

    private void handleListObjects(HttpExchange exchange, String bucketName, String prefix) throws IOException {
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

            respondOkAndClose(exchange);
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
            respondOkAndClose(exchange);
        }
    }

    static boolean bucketNameValid(String bucketName) {
        if (bucketName == null) {
            return false;
        }
        return BUCKET_NAME_MUST_MATCH_PATTERN.matcher(bucketName).matches()
                && !BUCKET_NAME_MUST_NOT_MATCH_PATTERN.matcher(bucketName).find();
    }

    private void handleDeleteObject(HttpExchange exchange, String bucketName, String keyName) throws IOException {
        readRequestBodyFully(exchange);

        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            bucket.remove(keyName);
        }
        respondNoContentAndClose(exchange);
    }

    private void handleDeleteBucket(HttpExchange exchange,String bucketName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            Bucket bucket = buckets.get(bucketName);
            if (bucket.isEmpty()) {
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

    private void respondFoundObjectAndClose(HttpExchange exchange, StoredObject storedObject) throws IOException {
        byte[] response = storedObject.getContent();

        addHeader(exchange, "ETag", "\"" + storedObject.md5HexString() + "\"");
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "text/plain");
        respondOkAndClose(exchange, response);
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

    private void respondOkAndClose(HttpExchange exchange) throws IOException {
        respondAndClose(exchange, HttpURLConnection.HTTP_OK, new byte[0]);
    }

    private void respondOkAndClose(HttpExchange exchange, byte[] response) throws IOException {
        respondAndClose(exchange, HttpURLConnection.HTTP_OK, response);
    }

    private void respondWithXmlDocumentAndClose(HttpExchange exchange, int httpCode, XmlDocument xmlDocument) throws IOException {
        xmlDocument.build();
        addHeader(exchange, HttpHeaders.CONTENT_TYPE, "application/xml");
        respondAndClose(exchange, httpCode, xmlDocument.toUtf8Bytes());
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
            InputStream inputStream = exchange.getRequestBody();
            int lengthRead = inputStream.read(content);
            inputStream.close();
            content = Arrays.copyOf(content, lengthRead);
            return content;
        } else {
            return new byte[0];
        }
    }
}
