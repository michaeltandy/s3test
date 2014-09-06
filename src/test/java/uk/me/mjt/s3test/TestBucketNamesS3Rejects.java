package uk.me.mjt.s3test;

import java.io.File;
import java.io.UnsupportedEncodingException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;

public class TestBucketNamesS3Rejects {

    public static void main(String[] args) throws Exception {
        new TestBucketNamesS3Rejects().testCombinations();
    }


    public void testCombinations() throws Exception {
        String filename = System.getProperty("user.home")+"/aws.properties";
        PropertiesCredentials credentials = new PropertiesCredentials(new File(filename));

        AmazonS3Client client = new AmazonS3Client(credentials);
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));

        testBucketName(client,"doesnotrxist.mjt.me.uk");
        testBucketName(client,"name-with-hyphens.mjt.me.uk");
        testBucketName(client,"name_underscore.mjt.me.uk");
        testBucketName(client,"-initial-hyphen.mjt.me.uk");
        testBucketName(client,".initial-dot.mjt.me.uk");
        testBucketName(client,"CAPITALLETTERS.mjt.me.uk");
        testBucketName(client,"dashdota.-.mjt.me.uk");
        testBucketName(client,"dashdotb-.mjt.me.uk");
        testBucketName(client,"dashdotc.-mjt.me.uk");
        testBucketName(client,"?.mjt.me.uk");
        testBucketName(client,"§.mjt.me.uk");
        testBucketName(client,"\u2605.mjt.me.uk");
        testBucketName(client,"\u3070.mjt.me.uk");
        testBucketName(client," ");
        testBucketName(client,"s");
        testBucketName(client,"");
        testBucketName(client,"asdf");
        testBucketName(client,"192.168.0.1.mjt.me.uk");
        testBucketName(client,"192.168.0");
        testBucketName(client,"192.168.0.1");
        testBucketName(client,"192.168.0.1.2");
        testBucketName(client,"292.168.0.1");
        testBucketName(client,"392.168.0.1");
        testBucketName(client,"1000.168.0.1");
        testBucketName(client,"10000.2.3.4");
        testBucketName(client,"100000.2.3.4");
        testBucketName(client,"1000000.2.3.4");
        testBucketName(client,"1-000.168.0.1");
        testBucketName(client,"2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        testBucketName(client,"asdfasdfasdfadsfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf.mjt.me.uk");
        testBucketName(client,"a");
        testBucketName(client,"aa");
        testBucketName(client,"aaa");
        testBucketName(client,"aaaa");
        testBucketName(client,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        testBucketName(client,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        testBucketName(client,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        testBucketName(client,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        testBucketName(client,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    public boolean testBucketName(AmazonS3Client client, String key) throws UnsupportedEncodingException {
        boolean success;

        try {
            com.amazonaws.services.s3.model.Bucket b = client.createBucket(key,com.amazonaws.services.s3.model.Region.EU_Ireland);
            System.out.println("OK/Success " + key);
            success = true;
        } catch (IllegalArgumentException e) {
            System.out.println("BAD A  " + key);
            success=false;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 403) {
                /*e.printStackTrace();
                System.out.println("Error Code: " + e.getErrorCode());
                System.out.println("Error Message: " + e.getErrorMessage());
                System.out.println("Status Code: " + e.getStatusCode());
                System.out.println("Exception caused by " + key);*/
                System.out.println("BAD B  " + key);
            } else {
                System.out.println("OK/403 " + key);
            }
            success = false;
        }

        if (success) {
            try {
                client.deleteBucket(key);
                return true;

            } catch (AmazonS3Exception e) {
                System.out.println("Problem cleaning up? " + key);
                return false;
            }
        } else {
            return false;
        }
    }

}
