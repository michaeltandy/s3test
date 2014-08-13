/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.me.mjt.s3test;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

/**
 *
 * @author mtandy
 */
public class BucketManipulationTest extends BasicTestSuperclass {
    
    private static final String bucketName = "newlycreatedbucket";
    
    public BucketManipulationTest() {
    }
    
    @Test
    public void testCreateBuckjet_thenPutAndGet() throws IOException {
        byte[] content = "qwer".getBytes("UTF-8");
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);
        
        client.createBucket(bucketName);
        
        PutObjectRequest s3request = new PutObjectRequest(bucketName, "qwer.txt",new ByteArrayInputStream(content),metadata);
        PutObjectResult response = client.putObject(s3request);
        
        assertNotNull(response);
        
        S3Object getResponse = client.getObject(bucketName, "qwer.txt");
        String getContent = inputStreamToString(getResponse.getObjectContent());
        assertEquals("qwer",getContent);
        
        client.deleteBucket(bucketName);
    }
    
    
    
}
