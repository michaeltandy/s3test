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
        Element rootElement = document.createElement("ListAllMyBucketsResult");
        document.appendChild(rootElement);

        Element owner = getOwnerElement();
        rootElement.appendChild(owner);

        Element bucketsElement = document.createElement("Buckets");
        rootElement.appendChild(bucketsElement);

        for (String bucketName : buckets.keySet()) {
            Bucket bucket = buckets.get(bucketName);
            Element bucketElement = getBucketElement(bucketName, bucket);
            bucketsElement.appendChild(bucketElement);
        }
    }

    private Element getOwnerElement() {
        Element owner = document.createElement("Owner");

        Element id = document.createElement("ID");
        id.appendChild(document.createTextNode(ownerId));
        owner.appendChild(id);

        Element displayName = document.createElement("DisplayName");
        displayName.appendChild(document.createTextNode(ownerDisplayName));
        owner.appendChild(displayName);

        return owner;
    }

    private Element getBucketElement(String bucketName, Bucket bucket) {
        Element bucketElement = document.createElement("Bucket");

        Element name = document.createElement("Name");
        name.appendChild(document.createTextNode(bucketName));
        bucketElement.appendChild(name);

        Element creationDate = document.createElement("CreationDate");
        creationDate.appendChild(document.createTextNode(bucket.getCreationDateString()));
        bucketElement.appendChild(creationDate);
        return bucketElement;
    }

}
