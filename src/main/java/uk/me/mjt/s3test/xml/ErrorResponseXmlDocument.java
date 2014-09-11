package uk.me.mjt.s3test.xml;

import org.w3c.dom.Element;
import uk.me.mjt.s3test.ErrorResponse;

public class ErrorResponseXmlDocument extends XmlDocument {

    private final ErrorResponse errorResponse;

    public ErrorResponseXmlDocument(ErrorResponse errorResponse) {
        this.errorResponse = errorResponse;
    }

    @Override
    public void build() {
        Element errorElement = document.createElement("Error");
        document.appendChild(errorElement);
        errorElement.appendChild(createElementWithText("Code", errorResponse.getCode()));
        errorElement.appendChild(createElementWithText("Message", errorResponse.getMessage()));
    }
}
