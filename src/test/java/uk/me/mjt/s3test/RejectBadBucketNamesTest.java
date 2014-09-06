package uk.me.mjt.s3test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazonaws.AmazonClientException;
import org.junit.Test;

public class RejectBadBucketNamesTest extends BasicTestSuperclass {

    @Test
    public void testShouldAccept() {
        assertTrue(bucketNameValid("doesnotexist.mjt.me.uk"));
        assertTrue(bucketNameValid("name-with-hyphens.mjt.me.uk"));
        assertTrue(bucketNameValid("192.168.0.1.mjt.me.uk"));
        assertTrue(bucketNameValid("1.2.3"));
        assertTrue(bucketNameValid("1.2.3.4.5"));
        assertTrue(bucketNameValid("1-0.2.3.4"));
        assertTrue(bucketNameValid("aaa"));
        assertTrue(bucketNameValid("aaaa"));
        assertTrue(bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assertTrue(bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    @Test
    public void testShouldReject() {
        assertFalse(bucketNameValid(null));
        assertFalse(bucketNameValid("name_underscore.mjt.me.uk"));
        assertFalse(bucketNameValid("-initial-hyphen.mjt.me.uk"));
        assertFalse(bucketNameValid(".initial-dot.mjt.me.uk"));
        assertFalse(bucketNameValid("CAPITALLETTERS.mjt.me.uk"));
        assertFalse(bucketNameValid("dashdota.-.mjt.me.uk"));
        assertFalse(bucketNameValid("dashdotb-.mjt.me.uk"));
        assertFalse(bucketNameValid("dashdotc.-mjt.me.uk"));
        assertFalse(bucketNameValid("?.mjt.me.uk"));
        assertFalse(bucketNameValid("§.mjt.me.uk"));
        assertFalse(bucketNameValid("\u2605.mjt.me.uk"));
        assertFalse(bucketNameValid("\u3070.mjt.me.uk"));
        assertFalse(bucketNameValid(" "));
        assertFalse(bucketNameValid("s"));
        assertFalse(bucketNameValid(""));
        assertFalse(bucketNameValid("192.168.0.1"));
        assertFalse(bucketNameValid("292.168.0.1"));
        assertFalse(bucketNameValid("392.168.0.1"));
        assertFalse(bucketNameValid("1000.168.0.1"));
        assertFalse(bucketNameValid("10000.2.3.4"));
        assertFalse(bucketNameValid("100000.2.3.4"));
        assertFalse(bucketNameValid("1000000.2.3.4"));
        assertFalse(bucketNameValid("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assertFalse(bucketNameValid("a"));
        assertFalse(bucketNameValid("aa"));
        assertFalse(bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    private boolean bucketNameValid(String bucketName) {
        try {
            client.createBucket(bucketName);
            return true;
        } catch (IllegalArgumentException | AmazonClientException e) {
            return false;
        }
    }

}
