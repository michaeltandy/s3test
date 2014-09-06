package uk.me.mjt.s3test.xml;

import org.w3c.dom.Element;
import uk.me.mjt.s3test.Bucket;

import java.util.Map;

public class ListBucketsXmlDocument extends XmlDocument {

    private final Map<String, Bucket> buckets;
    private final String ownerId;
    private final String ownerDisplayName;

    public ListBucketsXmlDocument(Map<String, Bucket> buckets,
                                  String ownerId,
                                  String ownerDisplayName) {
        this.buckets = buckets;
        this.ownerId = ownerId;
        this.ownerDisplayName = ownerDisplayName;
    }

    public void build() {
        Element listBucketsElement = document.createElement("ListAllMyBucketsResult");
        document.appendChild(listBucketsElement);

        listBucketsElement.appendChild(createOwnerElement());

        Element bucketsElement = document.createElement("Buckets");
        listBucketsElement.appendChild(bucketsElement);

        for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            bucketsElement.appendChild(createBucketElement(bucketName, bucket));
        }
    }

    private Element createOwnerElement() {
        Element owner = document.createElement("Owner");
        owner.appendChild(createElementWithText("ID", ownerId));
        owner.appendChild(createElementWithText("DisplayName", ownerDisplayName));
        return owner;
    }

    private Element createBucketElement(String bucketName, Bucket bucket) {
        Element bucketElement = document.createElement("Bucket");
        bucketElement.appendChild(createElementWithText("Name", bucketName));
        bucketElement.appendChild(createElementWithText("CreationDate", bucket.getCreationDateString()));
        return bucketElement;
    }

}
