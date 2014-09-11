package uk.me.mjt.s3test;

import java.net.HttpURLConnection;

public enum ErrorResponse {
    INVALID_URI("InvalidURI", "Couldn't parse the specified URI", HttpURLConnection.HTTP_BAD_REQUEST),
    INVALID_BUCKET_NAME("InvalidBucketName", "The specified bucket is not valid", HttpURLConnection.HTTP_BAD_REQUEST),
    NO_SUCH_BUCKET("NoSuchBucket", "The specified bucket does not exist", HttpURLConnection.HTTP_NOT_FOUND),
    NO_SUCH_KEY("NoSuchKey", "The specified key does not exist", HttpURLConnection.HTTP_NOT_FOUND),
    BUCKET_NOT_EMPTY("BucketNotEmpty", "The bucket you tried to delete is not empty", HttpURLConnection.HTTP_CONFLICT),
    BUCKET_ALREADY_EXISTS("BucketAlreadyExists", "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again", HttpURLConnection.HTTP_CONFLICT);

    private final String code;
    private final String message;
    private final int statusCode;

    private ErrorResponse(String code,
                          String message,
                          int statusCode) {
        this.code = code;
        this.message = message;
        this.statusCode = statusCode;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public int getStatusCode() {
        return statusCode;
    }

}
