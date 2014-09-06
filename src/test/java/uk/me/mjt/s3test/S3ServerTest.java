package uk.me.mjt.s3test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;

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

    @Test
    public void testDelete_thenGet() throws IOException {
        client.getObject("bucketname", "asdf.txt");
        client.deleteObject("bucketname", "asdf.txt");
        try {
            S3Object r2 = client.getObject("bucketname", "asdf.txt");
            fail("Should have thrown an exception by now??");
        } catch (AmazonS3Exception e) {
            assertEquals(404,e.getStatusCode());
            assertEquals("NoSuchKey",e.getErrorCode());
        }
    }

}
