package uk.me.mjt.s3test;

import java.io.File;

import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.junit.Test;

public class UploadDirectoryTest extends BasicTestSuperclass {

    @Test
    public void transferDirectory() throws Exception {
        File dirToUpload = new File(UploadDirectoryTest.class.getResource("/directoryToUpload").toURI());

        TransferManager tx = null;
        try {
            tx = new TransferManager(client);
            MultipleFileUpload upload = tx.uploadDirectory("bucketname", "targetDirectory", dirToUpload, true);
            upload.waitForCompletion();
        } finally {
            if (tx != null) tx.shutdownNow();
        }


    }
}
