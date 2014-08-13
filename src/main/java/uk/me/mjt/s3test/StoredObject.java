package uk.me.mjt.s3test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

/**
 *
 * @author mtandy
 */
public class StoredObject {
    private byte[] content;
    
    StoredObject(byte[] content) {
        this.content = content;
    }
    
    StoredObject(String s) {
        this(s.getBytes());
    }
    
    public byte[] getContent() {
        return content;
    }
    
    public String md5HexString() {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(content);
            return DatatypeConverter.printHexBinary(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This should never happen",e);
        }
    }
    
}
