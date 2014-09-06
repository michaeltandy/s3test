package uk.me.mjt.s3test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Test;

public class ListObjectsTest extends BasicTestSuperclass {

    @Test
    public void testEmptyBucketReturnsEmptyList() throws Exception {
        client.createBucket("test-bucket");
        ObjectListing objectListing = client.listObjects("test-bucket");
        assertTrue(objectListing.getObjectSummaries().isEmpty());
    }

    @Test
    public void testEmptyBucketReturnsExpectedObjects() throws Exception {
        client.createBucket("test-bucket");
        putObject("test-bucket", "file-1", "foobar");
        putObject("test-bucket", "file-2", "");
        putObject("test-bucket", "/file/with/slashes-1.xml", "foobar");
        putObject("test-bucket", "/file/with/loads/of/slashes-3.png", "dfsrgt3");

        ObjectListing objectListing = client.listObjects("test-bucket");

        List<String> objectKeys = new ArrayList<>();

        for(S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
            assertEquals("test-bucket", s3ObjectSummary.getBucketName());
            objectKeys.add(s3ObjectSummary.getKey());
        }

        assertEquals(4, objectKeys.size());
        assertTrue(objectKeys.contains("file-1"));
        assertTrue(objectKeys.contains("file-2"));
        assertTrue(objectKeys.contains("/file/with/slashes-1.xml"));
        assertTrue(objectKeys.contains("/file/with/loads/of/slashes-3.png"));
   }

    @Test
    public void testListObjectsWithPrefix() throws Exception {
        client.createBucket("test-bucket");
        putObject("test-bucket", "no-path-1", "foobar");
        putObject("test-bucket", "no-path-2", "");
        putObject("test-bucket", "/one/prefix-1.xml", "foobar");
        putObject("test-bucket", "/one/two/prefix-1.xml", "foobar");
        putObject("test-bucket", "/two/prefix-1.xml", "foobar");
        putObject("test-bucket", "/file/with/loads/of/slashes-3.png", "dfsrgt3");

        ObjectListing objectListing = client.listObjects("test-bucket", "one");

        List<String> objectKeys = new ArrayList<>();

        for(S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
            assertEquals("test-bucket", s3ObjectSummary.getBucketName());
            objectKeys.add(s3ObjectSummary.getKey());
        }

        assertEquals(2, objectKeys.size());
        assertTrue(objectKeys.contains("/one/prefix-1.xml"));
        assertTrue(objectKeys.contains("/one/two/prefix-1.xml"));
    }

    @Test
    public void testListObjectsWithNoPathWithPrefix() throws Exception {
        client.createBucket("test-bucket");
        putObject("test-bucket", "no-path-1", "foobar");
        putObject("test-bucket", "no-path-2", "");
        putObject("test-bucket", "/one/prefix-1.xml", "foobar");
        putObject("test-bucket", "/no-path/two/prefix-1.xml", "foobar");
        putObject("test-bucket", "/two/prefix-1.xml", "foobar");
        putObject("test-bucket", "/file/with/loads/of/slashes-3.png", "dfsrgt3");

        ObjectListing objectListing = client.listObjects("test-bucket", "no-path");

        List<String> objectKeys = new ArrayList<>();

        for(S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
            assertEquals("test-bucket", s3ObjectSummary.getBucketName());
            objectKeys.add(s3ObjectSummary.getKey());
        }

        assertEquals(3, objectKeys.size());
        assertTrue(objectKeys.contains("no-path-1"));
        assertTrue(objectKeys.contains("no-path-2"));
        assertTrue(objectKeys.contains("/no-path/two/prefix-1.xml"));
    }

    private void putObject(String bucketName, String name, String data) throws Exception {
        byte[] content = data.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);

        PutObjectRequest s3request = new PutObjectRequest(bucketName, name, new ByteArrayInputStream(content), metadata);
        client.putObject(s3request);
    }
}
