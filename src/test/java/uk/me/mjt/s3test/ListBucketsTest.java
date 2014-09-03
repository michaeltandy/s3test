package uk.me.mjt.s3test;

import com.amazonaws.services.s3.model.Bucket;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ListBucketsTest extends BasicTestSuperclass {
    @Test
    public void listBucketsOnEmpty() throws Exception {
        removeDefaultBucket();
        assertTrue(client.listBuckets().isEmpty());
    }

    @Test
    public void listBucketsShouldReturnExpectedBuckets() throws Exception {
        removeDefaultBucket();
        client.createBucket("bucket-1");
        client.createBucket("bucket-2");
        client.createBucket("bucket-3");
        client.createBucket("bucket-4");
        client.createBucket("bucket-5");

        List<com.amazonaws.services.s3.model.Bucket> buckets = client.listBuckets();
        List<String> bucketNames = new ArrayList<>();
        for(Bucket bucket : buckets) {
            bucketNames.add(bucket.getName());
        }

        assertEquals(5, bucketNames.size());
        assertTrue(bucketNames.contains("bucket-1"));
        assertTrue(bucketNames.contains("bucket-2"));
        assertTrue(bucketNames.contains("bucket-3"));
        assertTrue(bucketNames.contains("bucket-4"));
        assertTrue(bucketNames.contains("bucket-5"));

    }
}
