
package uk.me.mjt.s3test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

public class Bucket extends HashMap<String, StoredObject> {

    private final String name;
    private final Date creationDate;

    private static final DateFormat iso8601DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.'000Z'");

    static {
        iso8601DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public Bucket(String name) {
        this.name = name;
        this.creationDate = new Date();
    }

    public String getName() {
        return name;
    }

    public String getCreationDateString() {
        return iso8601DateFormat.format(creationDate);
    }
}
