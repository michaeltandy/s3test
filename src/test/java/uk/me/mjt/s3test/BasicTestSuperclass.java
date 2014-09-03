package uk.me.mjt.s3test;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.junit.After;
import org.junit.Before;

public class BasicTestSuperclass {
    
    S3Server instance;
    AmazonS3Client client;
    
    @Before
    public void setUp() throws Exception {
        instance = new S3Server();
        instance.start();
        client = new AmazonS3Client(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        client.setEndpoint(instance.getAddress());
        createDefaultBucket(client);
    }
    
    @After
    public void tearDown() throws Exception {
        client.shutdown();
        instance.stop();
    }

    public static void createDefaultBucket(AmazonS3Client client) throws Exception {
        client.createBucket("bucketname");

        byte[] content = "asdf".getBytes("UTF-8");
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);

        PutObjectRequest s3request = new PutObjectRequest("bucketname", "asdf.txt", new ByteArrayInputStream(content), metadata);
        client.putObject(s3request);
    }
    
    public void removeDefaultBucket() throws Exception {
        client.deleteBucket("bucketname");
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
