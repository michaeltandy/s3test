package uk.me.mjt.s3test.xml;

import org.w3c.dom.Element;
import uk.me.mjt.s3test.Bucket;
import uk.me.mjt.s3test.StoredObject;

public class ListObjectsXmlDocument extends XmlDocument {

    private Bucket bucket;
    private String prefix;
    private final String ownerId;
    private final String ownerDisplayName;

    public ListObjectsXmlDocument(Bucket bucket,
                                  String prefix,
                                  String ownerId,
                                  String ownerDisplayName) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.ownerId = ownerId;
        this.ownerDisplayName = ownerDisplayName;
    }

    @Override
    public void build() {
        Element rootElement = document.createElement("ListBucketResult");
        document.appendChild(rootElement);

        Element name = document.createElement("Name");
        name.appendChild(document.createTextNode(bucket.getName()));
        rootElement.appendChild(name);

        Element prefixElement = document.createElement("Prefix");
        prefixElement.appendChild(document.createTextNode(prefix));
        rootElement.appendChild(prefixElement);

        Element isTruncated = document.createElement("IsTruncated");
        isTruncated.appendChild(document.createTextNode("false"));
        rootElement.appendChild(isTruncated);

        for(String objectName : bucket.keySet()) {
            if(objectNameHasPrefix(prefix, objectName)) {
                StoredObject storedObject = bucket.get(objectName);
                Element contentsElement = getContentsElement(objectName, storedObject);
                rootElement.appendChild(contentsElement);
            }
        }
    }

    private Element getContentsElement(String objectName, StoredObject storedObject) {
        Element contents = document.createElement("Contents");

        Element key = document.createElement("Key");
        key.appendChild(document.createTextNode(objectName));
        contents.appendChild(key);

        Element eTag = document.createElement("ETag");
        eTag.appendChild(document.createTextNode(storedObject.md5HexString()));
        contents.appendChild(eTag);

        Element size = document.createElement("Size");
        size.appendChild(document.createTextNode(Integer.toString(storedObject.getContent().length)));
        contents.appendChild(size);

        Element ownerElement = getOwnerElement();
        contents.appendChild(ownerElement);

        Element storageClass = document.createElement("StorageClass");
        storageClass.appendChild(document.createTextNode("STANDARD"));
        contents.appendChild(storageClass);

        return contents;
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

    private boolean objectNameHasPrefix(String prefix, String objectName) {
        if(prefix.isEmpty()) {
            return true;
        }
        if(objectName.startsWith("/")) {
            objectName = objectName.substring(1);
        }
        return objectName.startsWith(prefix);
    }
}
