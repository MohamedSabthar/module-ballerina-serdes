package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * TupleMessageType.
 */
public class TupleMessageType extends MessageType {
    public TupleMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ?
                getCurrentFieldName() + TYPE_SEPARATOR + RECORD_BUILDER : recordType.getName();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        // Check for cyclic reference in ballerina record
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
            MessageType childMessageType = new RecordMessageType(recordType, nestedMessageBuilder,
                    getMessageGenerator());
            ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
            messageBuilder.addNestedMessage(nestedMessageDefinition);
        }
        addElementFieldInMessageBuilder(nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        String nestedMessageName = getCurrentFieldName() + SEPARATOR + MAP_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        MessageType childMessageType = new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator());
        generateNestedMessageDefinitionAndSetElementField(childMessageType);
    }

    @Override
    public void setTableField(TableType tableType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TABLE_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        MessageType childMessageType = new TableMessageType(tableType, nestedMessageBuilder, getMessageGenerator());
        generateNestedMessageDefinitionAndSetElementField(childMessageType);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap mapEntryBuilder instead of creating new nested message builder
        MessageType childMessageType = new ArrayMessageType(arrayType, messageBuilder, getMessageGenerator(),
                parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This adds the value field in wrapped mapEntryBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        MessageType childMessageType = new UnionMessageType(unionType, nestedMessageBuilder, getMessageGenerator());
        generateNestedMessageDefinitionAndSetElementField(childMessageType);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        MessageType childMessageType = new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator());
        generateNestedMessageDefinitionAndSetElementField(childMessageType);
    }

    private void generateNestedMessageDefinitionAndSetElementField(MessageType childMessageType) {
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        getMessageBuilder().addNestedMessage(nestedMessageDefinition);
        addElementFieldInMessageBuilder(childMessageType.getMessageBuilder().getName());
    }

    private void addElementFieldInMessageBuilder(String fieldTypeOrNestedMessageName) {
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                fieldTypeOrNestedMessageName, getCurrentFieldName(), getCurrentFieldNumber());
        getMessageBuilder().addField(messageField);
    }
}
