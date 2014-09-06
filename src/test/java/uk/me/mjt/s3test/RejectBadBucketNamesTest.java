package uk.me.mjt.s3test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class RejectBadBucketNamesTest {
    
    @Test
    public void testShouldAccept() {
        assertTrue(S3Server.bucketNameValid("doesnotexist.mjt.me.uk"));
        assertTrue(S3Server.bucketNameValid("name-with-hyphens.mjt.me.uk"));
        assertTrue(S3Server.bucketNameValid("192.168.0.1.mjt.me.uk"));
        assertTrue(S3Server.bucketNameValid("1.2.3"));
        assertTrue(S3Server.bucketNameValid("1.2.3.4.5"));
        assertTrue(S3Server.bucketNameValid("1-0.2.3.4"));
        assertTrue(S3Server.bucketNameValid("aaa"));
        assertTrue(S3Server.bucketNameValid("aaaa"));
        assertTrue(S3Server.bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assertTrue(S3Server.bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }
    
    @Test
    public void testShouldReject() {
        assertFalse(S3Server.bucketNameValid(null));
        assertFalse(S3Server.bucketNameValid("name_underscore.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("-initial-hyphen.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid(".initial-dot.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("CAPITALLETTERS.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("dashdota.-.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("dashdotb-.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("dashdotc.-mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("?.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("§.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("\u2605.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid("\u3070.mjt.me.uk"));
        assertFalse(S3Server.bucketNameValid(" "));
        assertFalse(S3Server.bucketNameValid("s"));
        assertFalse(S3Server.bucketNameValid(""));
        assertFalse(S3Server.bucketNameValid("192.168.0.1"));
        assertFalse(S3Server.bucketNameValid("292.168.0.1"));
        assertFalse(S3Server.bucketNameValid("392.168.0.1"));
        assertFalse(S3Server.bucketNameValid("1000.168.0.1"));
        assertFalse(S3Server.bucketNameValid("10000.2.3.4"));
        assertFalse(S3Server.bucketNameValid("100000.2.3.4"));
        assertFalse(S3Server.bucketNameValid("1000000.2.3.4"));
        assertFalse(S3Server.bucketNameValid("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assertFalse(S3Server.bucketNameValid("a"));
        assertFalse(S3Server.bucketNameValid("aa"));
        assertFalse(S3Server.bucketNameValid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }
    
}
