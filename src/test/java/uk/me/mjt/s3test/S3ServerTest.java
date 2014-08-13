package uk.me.mjt.s3test;

import com.amazonaws.services.s3.model.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author mtandy
 */
public class S3ServerTest extends BasicTestSuperclass {
    
    public S3ServerTest() {
    }
    
    @Test
    public void testBasicGet() {
        GetObjectRequest s3request = new GetObjectRequest("bucketname", "asdf.txt");
        S3Object response = client.getObject(s3request);
        String content = inputStreamToString(response.getObjectContent());
        assertEquals("asdf",content);
    }
    
    @Test
    public void testBasicPut_thenGet() throws IOException {
        byte[] content = "qwer".getBytes("UTF-8");
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);
        
        PutObjectRequest s3request = new PutObjectRequest("bucketname", "qwer.txt",new ByteArrayInputStream(content),metadata);
        PutObjectResult response = client.putObject(s3request);
        
        assertNotNull(response);
        
        S3Object getResponse = client.getObject("bucketname", "qwer.txt");
        String getContent = inputStreamToString(getResponse.getObjectContent());
        assertEquals("qwer",getContent);
    }
        
}
