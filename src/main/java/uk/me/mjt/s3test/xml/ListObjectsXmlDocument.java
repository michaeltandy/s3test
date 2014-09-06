package uk.me.mjt.s3test.xml;

import org.w3c.dom.Element;
import uk.me.mjt.s3test.Bucket;
import uk.me.mjt.s3test.StoredObject;

import java.util.Map;

public class ListObjectsXmlDocument extends XmlDocument {

    private final Bucket bucket;
    private final String prefix;
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
        Element listObjectsElement = document.createElement("ListBucketResult");
        document.appendChild(listObjectsElement);

        listObjectsElement.appendChild(createElementWithText("Name", bucket.getName()));
        listObjectsElement.appendChild(createElementWithText("Prefix", prefix));
        listObjectsElement.appendChild(createElementWithText("IsTruncated", "false"));

        for(Map.Entry<String, StoredObject> entry : bucket.entrySet()) {
            StoredObject storedObject = entry.getValue();
            if(nameStartsWithPrefix(storedObject.getName(), prefix)) {
                listObjectsElement.appendChild(createContentsElement(storedObject));
            }
        }
    }

    private Element createContentsElement(StoredObject storedObject) {
        Element contents = document.createElement("Contents");
        contents.appendChild(createElementWithText("Key", storedObject.getName()));
        contents.appendChild(createElementWithText("ETag", storedObject.md5HexString()));
        contents.appendChild(createElementWithText("Size", Integer.toString(storedObject.getContent().length)));
        contents.appendChild(createOwnerElement());
        contents.appendChild(createElementWithText("StorageClass", "STANDARD"));
        return contents;
    }

    private Element createOwnerElement() {
        Element ownerElement = document.createElement("Owner");
        ownerElement.appendChild(createElementWithText("ID", ownerId));
        ownerElement.appendChild(createElementWithText("DisplayName", ownerDisplayName));
        return ownerElement;
    }

    private boolean nameStartsWithPrefix(String name, String prefix) {
        if(prefix.isEmpty()) {
            return true;
        }
        if(name.startsWith("/")) {
            name = name.substring(1);
        }
        return name.startsWith(prefix);
    }
}
