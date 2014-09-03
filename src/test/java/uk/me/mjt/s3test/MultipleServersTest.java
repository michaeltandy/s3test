package uk.me.mjt.s3test;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import static org.junit.Assert.*;
import org.junit.Test;
import static uk.me.mjt.s3test.BasicTestSuperclass.inputStreamToString;

/**
 *
 * @author mtandy
 */
public class MultipleServersTest {
    
    public MultipleServersTest() {
    }
    
    @Test
    public void testStartTwoServers() throws Exception {
        S3Server instanceA = null;
        S3Server instanceB = null;
        AmazonS3Client client = null;
        
        try {
            instanceA = new S3Server();
            instanceB = new S3Server();

            instanceA.start();
            instanceB.start();

            client = new AmazonS3Client(new StaticCredentialsProvider(new AnonymousAWSCredentials()));
            client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
            client.setEndpoint(instanceA.getAddress());
            BasicTestSuperclass.createDefaultBucket(client);
            S3Object response = client.getObject("bucketname", "asdf.txt");
            String content = inputStreamToString(response.getObjectContent());
            assertEquals("asdf",content);
            
            assertFalse(instanceA.getAddress().equals(instanceB.getAddress()));
            
        } finally {
            if (client!=null)
                client.shutdown();
            if (instanceA!=null)
                instanceA.stop();
            if (instanceB!=null)
                instanceB.stop();
        }
    }
        
}
