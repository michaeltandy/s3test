package uk.me.mjt.s3test;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 * @author mtandy
 */
public class TestKeyNamesS3Rejects {
    
    public static void main(String[] args) throws Exception {
        new TestKeyNamesS3Rejects().testCombinations();
        //new TestKeyNamesS3Rejects().testDoubleDots();
        
    }
    
    final ArrayList<String> prefixTestResults = new ArrayList<>();
    
    public void testDoubleDots() throws Exception {
        String filename = System.getProperty("user.home")+"/aws.properties";
        PropertiesCredentials credentials = new PropertiesCredentials(new File(filename));
        
        AmazonS3Client client = new AmazonS3Client(credentials);
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
        
        testPrefix(client,"",6);
        
        Collections.sort(prefixTestResults);
        for (String s : prefixTestResults) {
            System.out.println(s);
        }
        
    }
    
    public void testPrefix(AmazonS3Client client, String prefix, int depth) throws UnsupportedEncodingException {
        if (testKey(client, prefix)) {
            prefixTestResults.add(prefix + " -> OK");
        } else {
            prefixTestResults.add(prefix + " -> FAILED");
        }
        if (depth > 0) {
            testPrefix(client, prefix + "..", depth-1);
            testPrefix(client, prefix + "/", depth-1);
            testPrefix(client, prefix + "asdf", depth-1);
        }
    }
    
    public void testCombinations() throws Exception {
        String filename = System.getProperty("user.home")+"/aws.properties";
        PropertiesCredentials credentials = new PropertiesCredentials(new File(filename));
        
        AmazonS3Client client = new AmazonS3Client(credentials);
        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
        
        testKey(client,"asdf.txt");
        testKey(client," ");
        testKey(client,"qwer.txt?acl");
        testKey(client,"../../../..");
        testKey(client,"../../../.");
        testKey(client,"../../../");
        testKey(client,"../../..");
        testKey(client,"../../.");
        testKey(client,"../../");
        testKey(client,"../..");
        testKey(client,"../.");
        testKey(client,"../");
        testKey(client,"..");
        testKey(client,".");
        testKey(client,"./");
        testKey(client,"./.");
        testKey(client,"././");
        testKey(client,"././.");
        testKey(client,"////");
        testKey(client,"/");
        testKey(client,"\\\\\\");
        testKey(client,"\\");
        testKey(client,"");
        testKey(client,"\0");
        testKey(client,"~");
        testKey(client,"#");
        testKey(client,"@");
        testKey(client,";");
        testKey(client,":");
        testKey(client,"}");
        testKey(client,"]");
        testKey(client,"{");
        testKey(client,"[");
        testKey(client,"'");
        testKey(client,"\"");
        testKey(client,"+");
        testKey(client,"=");
        testKey(client,"%");
        
        testKey(client,"\u00a5");
        testKey(client,"\u030a");
        testKey(client,"\r");
        testKey(client,"\n");
        testKey(client,"\r\t");
        testKey(client,"\t");
        testKey(client,";");
        testKey(client,"?");
        testKey(client,"\01");
        testKey(client,"\b");
        testKey(client,"\uFFFD");
        testKey(client,"\u0080");
        testKey(client,"\uDC80");
        testKey(client,"\uD800");
        testKey(client,"\u200F");
        testKey(client,"\u009F");
        testKey(client,"\u00a0");
        testKey(client,"\u2060");
        testKey(client,"\uFEFF");
        
        testKey(client,"asdf../..");
        testKey(client,"asdf/../..");
        testKey(client,"asdf/../../");
        testKey(client,"asdf/../../..");
        testKey(client,"../..asdf");
        testKey(client,"../../asdf");
        testKey(client,"..asdf/..");
        testKey(client,"..asdf/asdf..");
        testKey(client,"../asdf/..");
    }
    
    public boolean testKey(AmazonS3Client client, String key) throws UnsupportedEncodingException {
        String messageContents = "The key is >" +key + "< or, as bytes, " + Arrays.toString(key.getBytes());
        byte[] content = messageContents.getBytes("UTF-8");
        
        boolean putSuccess;
        
        try {            
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            
            PutObjectRequest s3request = new PutObjectRequest("s3-test-mjt", key,new ByteArrayInputStream(content),metadata);
            PutObjectResult response = client.putObject(s3request);
            
            putSuccess = true;
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Error Message: " + e.getErrorMessage());
            System.out.println("Status Code: " + e.getStatusCode());
            System.out.println("Problem during put. " + messageContents);
            putSuccess = false;
        }
        
        if (putSuccess) {
            try {
                GetObjectRequest getRequest = new GetObjectRequest("s3-test-mjt", key);
                S3Object getResponse = client.getObject(getRequest);
                InputStream is = getResponse.getObjectContent();
                byte[] readback = new byte[content.length];
                is.read(readback);
                is.close();
                getResponse.close();
                
                DeleteObjectRequest deleteRequest = new DeleteObjectRequest("s3-test-mjt", key);
                client.deleteObject(deleteRequest);

                if (Arrays.equals(content, readback)) {
                    // Looks OK with this key.
                    return true;
                } else {
                    System.out.println("Readback failure. " + messageContents);
                    return false;
                }

            } catch (AmazonS3Exception e) {
                System.out.println("Problem during readback. " + messageContents);
                return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } else {
            return false;
        }
    }
    
}
