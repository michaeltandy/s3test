package uk.me.mjt.s3test;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.ByteArrayInputStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import org.junit.Test;

public class RejectBadKeyNamesTest extends BasicTestSuperclass {
    
    public RejectBadKeyNamesTest() {
    }
    
    @Test
    public void testShouldAccept() {
        testShouldAllowKey("asdf");
        testShouldAllowKey("..");
        testShouldAllowKey("....");
        testShouldAllowKey("....asdfasdfasdfasdf");
        testShouldAllowKey("../");
        testShouldAllowKey("../....");
        testShouldAllowKey("../......");
        testShouldAllowKey("../....asdf/");
        testShouldAllowKey("../....asdfasdf");
        testShouldAllowKey("../..asdf");
        testShouldAllowKey("../..asdf..");
        testShouldAllowKey("../..asdfasdfasdf");
        testShouldAllowKey("..//");
        testShouldAllowKey("..//....");
        testShouldAllowKey("..//......");
        testShouldAllowKey("..//..../");
        testShouldAllowKey("..//....asdf");
        testShouldAllowKey("..//..asdf");
        testShouldAllowKey("..//..asdf..");
        testShouldAllowKey("..//..asdfasdf");
        testShouldAllowKey("..///");
        testShouldAllowKey("..///....");
        testShouldAllowKey("..///....");
        testShouldAllowKey("..///..asdf");
        testShouldAllowKey("..////");
        testShouldAllowKey("..///..asdf");
        testShouldAllowKey("..////");
        testShouldAllowKey("../////");
        testShouldAllowKey("..////asdf");
        testShouldAllowKey("/....asdfasdfasdf");
        testShouldAllowKey("/../");
        testShouldAllowKey("/../....");
        testShouldAllowKey("/../......");
        testShouldAllowKey("/../..../");
        testShouldAllowKey("/../....asdf");
        testShouldAllowKey("/../..asdf");
        testShouldAllowKey("/../..asdf..");
        testShouldAllowKey("/../..asdfasdf");
        testShouldAllowKey("/..//");
        testShouldAllowKey("/..//....");
        testShouldAllowKey("/..//....");
        testShouldAllowKey("/..//..asdf");
        testShouldAllowKey("/..///");
        testShouldAllowKey("/..//..asdf");
        testShouldAllowKey("/..///");
        testShouldAllowKey("/..////");
        testShouldAllowKey("/..///asdf");
        testShouldAllowKey("//....asdfasdf");
        testShouldAllowKey("//../");
        testShouldAllowKey("//../....");
        testShouldAllowKey("//../....");
        testShouldAllowKey("//../..asdf");
        testShouldAllowKey("//..//");
        testShouldAllowKey("//../..asdf");
        testShouldAllowKey("//..//");
        testShouldAllowKey("//..///");
        testShouldAllowKey("//..//asdf");
        testShouldAllowKey("///....asdf");
        testShouldAllowKey("///../");
        testShouldAllowKey("///..//");
        testShouldAllowKey("///../asdf");
    }
    
    @Test
    public void testShouldRefuse() {
        testShouldRefuseKey_InvalidUri("../..");
        testShouldRefuseKey_InvalidUri("../../");
        testShouldRefuseKey_InvalidUri("../../..");
        testShouldRefuseKey_InvalidUri("../../....");
        testShouldRefuseKey_InvalidUri("../../../");
        testShouldRefuseKey_InvalidUri("../../..asdf");
        testShouldRefuseKey_InvalidUri("../..//");
        testShouldRefuseKey_InvalidUri("../..//..");
        testShouldRefuseKey_InvalidUri("../..///");
        testShouldRefuseKey_InvalidUri("../..//asdf");
        testShouldRefuseKey_InvalidUri("../../asdf");
        testShouldRefuseKey_InvalidUri("../../asdf..");
        testShouldRefuseKey_InvalidUri("../../asdf/");
        testShouldRefuseKey_InvalidUri("../../asdfasdf");
        testShouldRefuseKey_InvalidUri("..//..");
        testShouldRefuseKey_InvalidUri("..//../");
        testShouldRefuseKey_InvalidUri("..//../..");
        testShouldRefuseKey_InvalidUri("..//..//");
        testShouldRefuseKey_InvalidUri("..//../asdf");
        testShouldRefuseKey_InvalidUri("..///..");
        testShouldRefuseKey_InvalidUri("..///../");
        testShouldRefuseKey_InvalidUri("..////..");
        testShouldRefuseKey_InvalidUri("/../..");
        testShouldRefuseKey_InvalidUri("/../../");
        testShouldRefuseKey_InvalidUri("/../../..");
        testShouldRefuseKey_InvalidUri("/../..//");
        testShouldRefuseKey_InvalidUri("/../../asdf");
        testShouldRefuseKey_InvalidUri("/..//..");
        testShouldRefuseKey_InvalidUri("/..//../");
        testShouldRefuseKey_InvalidUri("/..///..");
        testShouldRefuseKey_InvalidUri("//../..");
        testShouldRefuseKey_InvalidUri("//../../");
        testShouldRefuseKey_InvalidUri("//..//..");
        testShouldRefuseKey_InvalidUri("///../..");
    }
    
    public void testShouldAllowKey(String key) {
        try {
            byte[] content = "qwer".getBytes();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);

            PutObjectRequest s3request = new PutObjectRequest("bucketname", key,new ByteArrayInputStream(content),metadata);
            PutObjectResult response = client.putObject(s3request);

            assertNotNull(response);
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            fail("Wrongly refused key " + key);
        }
    }
    
    public void testShouldRefuseKey_InvalidUri(String key) {
        try {
            byte[] content = "qwer".getBytes();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);

            PutObjectRequest s3request = new PutObjectRequest("bucketname", key,new ByteArrayInputStream(content),metadata);
            PutObjectResult response = client.putObject(s3request);

            fail("Wrongly allowed key " + key);
        } catch (AmazonS3Exception e) {
            assertEquals(400,e.getStatusCode());
            assertEquals("400 Invalid URI",e.getErrorCode());
        }
    }
    
}
