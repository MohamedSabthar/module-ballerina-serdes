package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.Type;

/**
 * {@link MessageFieldData} holds the ballerina value, ballerina type and generated field name of a protobuf field.
 */
public class MessageFieldData {
    private final String fieldName;
    private final Object ballerinaValue;
    private final Type ballerinaType;

    public MessageFieldData(String fieldName, Object ballerinaValue, Type ballerinaType) {
        this.fieldName = fieldName;
        this.ballerinaValue = ballerinaValue;
        this.ballerinaType = ballerinaType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getBallerinaValue() {
        return ballerinaValue;
    }

    public Type getBallerinaType() {
        return ballerinaType;
    }
}
