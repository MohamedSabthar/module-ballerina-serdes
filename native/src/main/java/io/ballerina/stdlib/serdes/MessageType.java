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

import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * {@link MessageType} provides generic functions for concrete messageTypes.
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
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(integerType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(byteType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    public void setFloatField(FloatType floatType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(floatType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder nestedMessageBuilder = generateDecimalMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(decimalType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
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
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(stringType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    public void setBooleanField(BooleanType booleanType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(booleanType.getTag());
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    public abstract List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList();

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

    public void addNestedMessageDefinitionInMessageBuilder(RecordType recordType, String recordMessageName) {
        boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(recordMessageName);
        // Avoid recursive message definition for ballerina record with cyclic reference
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(recordMessageName, messageBuilder);
            MessageType childMessageType = new RecordMessageType(recordType, nestedMessageBuilder, messageGenerator);
            ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
            messageBuilder.addNestedMessage(nestedMessageDefinition);
        }
    }

    public void addNestedMessageDefinitionInMessageBuilder(TupleType tupleType, String tupleMessageName) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(tupleMessageName, messageBuilder);
        MessageType childMessageType = new TupleMessageType(tupleType, nestedMessageBuilder, messageGenerator);
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(nestedMessageDefinition);
    }

    public void addNestedMessageDefinitionInMessageBuilder(UnionType unionType, String unionMessageName) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(unionMessageName, messageBuilder);
        MessageType childMessageType = new UnionMessageType(unionType, nestedMessageBuilder, messageGenerator);
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(nestedMessageDefinition);
    }

    public void addNestedMessageDefinitionInMessageBuilder(TableType tableType, String tableMessageName) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(tableMessageName, messageBuilder);
        MessageType childMessageType = new TableMessageType(tableType, nestedMessageBuilder, messageGenerator);
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(nestedMessageDefinition);
    }

    public void addNestedMessageDefinitionInMessageBuilder(ArrayType arrayType, String arrayMessageName) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(arrayMessageName, messageBuilder);
        MessageType parentMessageType = messageGenerator.getMessageType();
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, nestedMessageBuilder,
                messageGenerator, parentMessageType);
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(nestedMessageDefinition);
    }

    public void addNestedMessageDefinitionInMessageBuilder(MapType mapType, String mapMessageName) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(mapMessageName, messageBuilder);
        MessageType childMessageType = new MapMessageType(mapType, nestedMessageBuilder, messageGenerator);
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(nestedMessageDefinition);
    }

    public ProtobufMessageBuilder getNestedMessageDefinition(MessageType childMessageType) {
        MessageType parentMessageType = messageGenerator.getMessageType();
        // switch to child message type
        messageGenerator.setMessageType(childMessageType);
        ProtobufMessageBuilder childMessageBuilder = messageGenerator.generateMessageDefinition();
        // switch back to parent message type
        messageGenerator.setMessageType(parentMessageType);
        return childMessageBuilder;
    }

    public void addMessageFieldInMessageBuilder(String fieldLabel, String fieldType) {
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(fieldLabel, fieldType,
                currentFieldName, currentFieldNumber);
        messageBuilder.addField(messageField);
    }
}
