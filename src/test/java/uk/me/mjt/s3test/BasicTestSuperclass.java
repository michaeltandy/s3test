package uk.me.mjt.s3test;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import java.io.IOException;
import java.io.InputStream;
import org.junit.After;
import org.junit.Before;

public class BasicTestSuperclass {
    
    S3Server instance;
    AmazonS3Client client;
    
    public BasicTestSuperclass() {
    }
    
    @Before
    public void setUp() throws IOException, InterruptedException {
        instance = new S3Server();
        instance.start();
        Thread.sleep(100);
        client = new AmazonS3Client(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        //client.setEndpoint("http://localhost:8123");
        client.setEndpoint(instance.getAddress());
    }
    
    @After
    public void tearDown() throws InterruptedException {
        client.shutdown();
        instance.stop();
        Thread.sleep(1000);
    }
    
    static String inputStreamToString(InputStream is) {
        // From http://stackoverflow.com/a/5445161/1367431
        try {
            java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
            return s.hasNext() ? s.next() : "";
        } finally {
            try {
                is.close();
            } catch (IOException e) {}
        }
    }
    
}
