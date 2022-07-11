package io.ballerina.stdlib.serdes;


import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * MessageType.
 */
public abstract class MessageType {
    private final ProtobufMessageBuilder messageBuilder;
    private final BallerinaStructuredTypeMessageGenerator messageGenerator;
    private Type ballerinaType;
    private String currentFieldName;
    private int currentFieldNumber;

    public MessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                       BallerinaStructuredTypeMessageGenerator messageGenerator) {
        this.messageBuilder = messageBuilder;
        this.ballerinaType = ballerinaType;
        this.messageGenerator = messageGenerator;
        setCurrentFieldNumber(1);
    }

    public void setIntField(IntegerType integerType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(integerType.getTag());
        getMessageBuilder().addField(messageField);
    }

    public void setByteField(ByteType byteType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(byteType.getTag());
        getMessageBuilder().addField(messageField);
    }

    public void setFloatField(FloatType floatType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(floatType.getTag());
        getMessageBuilder().addField(messageField);
    }

    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder nestedMessageBuilder = generateDecimalMessageDefinition();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(decimalType.getTag());
        getMessageBuilder().addField(messageField);
    }

    public ProtobufMessageBuilder generateDecimalMessageDefinition() {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(DECIMAL_VALUE);

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, 1);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                2);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, 3);

        nestedMessageBuilder.addField(scaleField);
        nestedMessageBuilder.addField(precisionField);
        nestedMessageBuilder.addField(valueField);

        return nestedMessageBuilder;
    }

    public void setStringField(StringType stringType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(stringType.getTag());
        getMessageBuilder().addField(messageField);
    }

    public void setBooleanField(BooleanType booleanType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldForBallerinaPrimitiveType(booleanType.getTag());
        getMessageBuilder().addField(messageField);
    }

    private ProtobufMessageFieldBuilder generateMessageFieldForBallerinaPrimitiveType(int typeTag) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(typeTag);
        return new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, getCurrentFieldName(), currentFieldNumber);
    }

    public void setEnumField(FiniteType finiteType) {
        throw new UnsupportedOperationException();
    }

    public void setNullField(NullType nullType) {
        throw new UnsupportedOperationException();
    }

    public abstract void setRecordField(RecordType recordType);

    public abstract void setMapField(MapType mapType);

    public abstract void setTableField(TableType tableType);

    public abstract void setArrayField(ArrayType arrayType);

    public abstract void setUnionField(UnionType unionType);

    public abstract void setTupleField(TupleType tupleType);

    public Type getBallerinaType() {
        return ballerinaType;
    }

    public void setBallerinaType(Type ballerinaType) {
        this.ballerinaType = ballerinaType;
    }

    public ProtobufMessageBuilder getMessageBuilder() {
        return messageBuilder;
    }

    public BallerinaStructuredTypeMessageGenerator getMessageGenerator() {
        return messageGenerator;
    }

    public String getCurrentFieldName() {
        return currentFieldName;
    }

    public void setCurrentFieldName(String fieldName) {
        this.currentFieldName = fieldName;
    }

    public int getCurrentFieldNumber() {
        return currentFieldNumber;
    }

    public void setCurrentFieldNumber(int currentFieldNumber) {
        this.currentFieldNumber = currentFieldNumber;
    }

    public void incrementFieldNumber() {
        ++currentFieldNumber;
    }

    public ProtobufMessageBuilder getNestedMessageDefinition(MessageType childMessageType) {
        MessageType parentMessageType = getMessageGenerator().getMessageType();
        // switch to child message type
        getMessageGenerator().setMessageType(childMessageType);
        ProtobufMessageBuilder childMessageBuilder = getMessageGenerator().generateMessageDefinition();
        // switch back to parent message type
        getMessageGenerator().setMessageType(parentMessageType);
        return childMessageBuilder;
    }
}
