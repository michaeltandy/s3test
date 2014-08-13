
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
    private final HttpServer hs;
    private final HashMap<String,Bucket> buckets = new HashMap();
    private ExecutorService es = null;
    private InetSocketAddress addr;
    
    private static final Pattern createBucketPattern = Pattern.compile("/([^/]+)/"); // Format is like "/bucketname/"
    private static final Pattern requestPathPattern = Pattern.compile("/([^/]+)/(.+)"); // Format is like "/bucketname/asdf.txt"
    private static final Pattern doubleDotPattern = Pattern.compile("(.*/)?\\.\\./+\\.\\.(/.*)?");
    
    // Bucket names must follow DNS rules from http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    private static final Pattern bucketNameMustMatchPattern = Pattern.compile("[a-z0-9\\.\\-]{3,63}");
    private static final Pattern bucketNameMustNotMatchPattern = Pattern.compile("(^[\\.\\-])|"
                                                                        + "([\\.\\-]$)|"
                                                                        + "([\\.\\-]{2})|"
                                                                        + "(^\\d+\\.\\d+\\.\\d+\\.\\d+$)");
    
    S3Server() throws IOException {
        this(null);
    }
    
    S3Server(InetSocketAddress addr) throws IOException {
        hs = HttpServer.create();
        this.addr = addr;
        createContext();
        
        Bucket defaultBucket = new Bucket();
        defaultBucket.put("asdf.txt", new StoredObject("asdf"));
        buckets.put("bucketname", defaultBucket);
    }
    
    public void start() throws IOException {
        es = Executors.newFixedThreadPool(3, new ThreadFactory() {
            int thisServerThreadCount = 0;
            public Thread newThread(Runnable r) {
                return new Thread(r, "S3Server thread " + (++thisServerThreadCount));
            }
        });
        hs.setExecutor(es);
        
        if (addr != null) {
            hs.bind(addr, 0);
        } else {
            bindToRandomPort();
        }
        hs.start();
    }
        
    public void stop() {
        hs.stop(1);
        es.shutdown();
        es = null;
    }
    
    public String getAddress() {
        return "http://"+addr.getHostName() + ":" + addr.getPort();
    }
    
    private void createContext() {
        // create and register our handler
        hs.createContext("/",new HttpHandler() {
            public void handle(HttpExchange exchange) throws IOException {
                switch(exchange.getRequestMethod()) {
                    case "GET" : handleGet(exchange); break;
                    case "PUT" : handlePut(exchange); break;
                    case "DELETE" : handleDelete(exchange); break;
                    default : 
                        System.out.println("Don't know how to " + exchange.getRequestMethod());
                        throw new UnsupportedOperationException("Don't know how to " + exchange.getRequestMethod());
                }
            }
        });
    }
    
    private void handleGet(HttpExchange exchange) throws IOException {
        System.out.println("GET "+exchange.getRequestURI());
        String path = exchange.getRequestURI().getPath();
        Matcher m = requestPathPattern.matcher(path);
        if (!m.matches()) {
            // FIXME what does AWS do in this case?
            System.out.println("Couldn't parse " + path + " ?!?!");
            returnInvalidUrl(exchange);
            return;
        }
        String bucketName = m.group(1);
        String keyName = m.group(2);
        
        if (doubleDotPattern.matcher(keyName).matches()) {
            returnInvalidUrl(exchange);
            return;
        }
        
        if (buckets.containsKey(bucketName)) {
            Bucket b = buckets.get(bucketName);
            if (b.containsKey(keyName)) {
                returnFoundObject(exchange,b.get(keyName));
            } else {
                System.out.println("Key " + keyName + " not found?");
            }
        } else {
            System.out.println("Bucket " + bucketName + " not found?");
        }
    }
    
    private void returnFoundObject(HttpExchange exchange, StoredObject obj) throws IOException {
        byte[] response = obj.getContent();
        
        addHeader(exchange, "ETag","\""+obj.md5HexString()+"\"");
        addHeader(exchange, "Content-Type","text/plain");
        respondOkAndClose(exchange,response);
    }
    
    private void returnInvalidUrl(HttpExchange exchange) throws IOException {
        byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<Error>\n" +
                            "  <Code>400 Invalid URI</Code>\n" +
                            "  <Message>URI was invalid?</Message>\n" +
                            "</Error>").getBytes();
        
        addHeader(exchange, "Content-Type","application/xml");
        respondAndClose(exchange,400,response);
    }
    
    private void handlePut(HttpExchange exchange) throws IOException {
        System.out.println("PUT "+exchange.getRequestURI());
        String path = exchange.getRequestURI().getPath();
        
        Matcher bucket = createBucketPattern.matcher(path);
        if (bucket.matches()) {
            String bucketName = bucket.group(1);
            handlePutBucket(exchange,bucketName);
            return;
        }
        
        Matcher m = requestPathPattern.matcher(path);
        if (m.matches()) {
            String bucketName = m.group(1);
            String keyName = m.group(2);
            handlePutObject(exchange,bucketName,keyName);
            return;
        }
        
        System.out.println("Couldn't parse " + path + " ?!?!");
        returnInvalidUrl(exchange);
        
    }
    
    private void handlePutObject(HttpExchange exchange,String bucketName,String keyName) throws IOException {
        if (doubleDotPattern.matcher(keyName).matches()) {
            returnInvalidUrl(exchange);
            return;
        }
        
        byte[] content = readRequestBodyFully(exchange);
        
        if (buckets.containsKey(bucketName)) {
            Bucket b = buckets.get(bucketName);
            StoredObject obj = new StoredObject(content);
            b.put(keyName, obj);
            addHeader(exchange, "ETag","\""+obj.md5HexString()+"\"");
            respondOkAndClose(exchange,new byte[0]);
        } else {
            System.out.println("Bucket " + bucketName + " not found?");
        }
    }
    
    private void handlePutBucket(HttpExchange exchange,String bucketName) throws IOException {
        byte[] reqBody = readRequestBodyFully(exchange);
        
        if (!bucketNameValid(bucketName)) {
            returnInvalidBucketName(exchange);
            
        } else if (buckets.containsKey(bucketName)) {
            System.out.println("Bucket " + bucketName + " already exists?");
            
            // FIXME send an error response body?
            respondAndClose(exchange,HttpURLConnection.HTTP_CONFLICT,new byte[0]);
        } else {
            System.out.println("Creating bucket " + bucketName + ".");
            buckets.put(bucketName, new Bucket());
            addHeader(exchange, "Location","/"+bucketName);
            respondOkAndClose(exchange,new byte[0]);
        }
        
    }
    
    static boolean bucketNameValid(String bucketName) {
        if (bucketName == null) return false;
        Matcher m1 = bucketNameMustMatchPattern.matcher(bucketName);
        Matcher m2 = bucketNameMustNotMatchPattern.matcher(bucketName);
        return m1.matches() && !m2.find();
    }
    
    private void handleDelete(HttpExchange exchange) throws IOException {
        System.out.println("DELETE " + exchange.getRequestURI());
        String path = exchange.getRequestURI().getPath();
        
        Matcher bucket = createBucketPattern.matcher(path);
        if (bucket.matches()) {
            String bucketName = bucket.group(1);
            handleDeleteBucket(exchange,bucketName);
            return;
        }
        
        Matcher m = requestPathPattern.matcher(path);
        if (m.matches()) {
            String bucketName = m.group(1);
            String keyName = m.group(2);
            handleDeleteObject(exchange,bucketName,keyName);
            return;
        }
        
        System.out.println("Couldn't parse " + path + " ?!?!");
        returnInvalidUrl(exchange);
        
    }
    
    private void handleDeleteObject(HttpExchange exchange,String bucketName,String keyName) throws IOException {
                
        byte[] content = readRequestBodyFully(exchange);
        
        if (buckets.containsKey(bucketName)) {
            Bucket b = buckets.get(bucketName);
            b.remove(keyName);
            respondNoContentAndClose(exchange);
        } else {
            System.out.println("Bucket " + bucketName + " not found?");
        }
    }
    
    private void handleDeleteBucket(HttpExchange exchange,String bucketName) throws IOException {
        if (buckets.containsKey(bucketName)) {
            buckets.remove(bucketName);
            respondNoContentAndClose(exchange);
        } else {
            System.out.println("Can't find bucket " + bucketName + " to delete?");
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
    
    private void returnInvalidBucketName(HttpExchange exchange) throws IOException {
        byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                            "<Error>\n" +
                            "  <Code>400 InvalidBucketName</Code>\n" +
                            "  <Message>The specified bucket is not valid.</Message>\n" +
                            "</Error>").getBytes();
        
        addHeader(exchange, "Content-Type","application/xml");
        respondAndClose(exchange,400,response);
    }
    
    private void respondOkAndClose(HttpExchange exchange, byte[] response) throws IOException {
        respondAndClose(exchange,HttpURLConnection.HTTP_OK,response);
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
        String lengthHeader = exchange.getRequestHeaders().getFirst("Content-Length");
        if (lengthHeader == null) lengthHeader="0";
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
        Random r = new Random();
        for (int i=0 ; i<20 ; i++) {
            if (attemptToBind(8000+r.nextInt(1000))) {
                return;
            }
        }
        throw new IOException("Made several attempts to bind to a randomly "
                + "chosen port and failed each time. Weird.");
    }
    
    private boolean attemptToBind(int port) throws IOException {
        try {
            InetSocketAddress binding = new InetSocketAddress(InetAddress.getLoopbackAddress(),port);
            hs.bind(binding, 0);
            addr = binding;
            return true;
        } catch (BindException e) {
            return false;
        }
    }
    
}
