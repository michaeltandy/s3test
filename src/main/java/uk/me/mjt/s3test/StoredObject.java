package uk.me.mjt.s3test;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class StoredObject {

    private final String name;
    private final byte[] content;
    
    StoredObject(String name,
                 byte[] content) {
        this.name = name;
        this.content = content;
    }
    
    public String getName() {
        return name;
    }

    public byte[] getContent() {
        return content;
    }
    
    public String md5HexString() {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] digest = messageDigest.digest(content);
            return DatatypeConverter.printHexBinary(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("This should never happen",e);
        }
    }
    
}
