package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * UnionMessageType.
 */
public class UnionMessageType extends MessageType {
    public UnionMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setEnumField(FiniteType finiteType) {
        addMemberFieldInMessageBuilder(STRING);
    }

    @Override
    public void setNullField(NullType nullType) {
        addMemberFieldInMessageBuilder(BOOL);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        if (isAnonymousBallerinaRecord(recordType)) {
            throw createSerdesError(Utils.typeNotSupportedErrorMessage(recordType), SERDES_ERROR);
        }

        String nestedMessageName = recordType.getName();
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
        addMemberFieldInMessageBuilder(nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        // TODO: add support to map type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + mapType.getName(), SERDES_ERROR);
    }

    @Override
    public void setTableField(TableType tableType) {
        // TODO: add support to table type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + tableType.getName(), SERDES_ERROR);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap existing message builder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, messageBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This adds the value field in wrapped messageBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = tupleType.getName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        MessageType childMessageType = new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder childMessageDefinition = getNestedMessageDefinition(childMessageType);
        messageBuilder.addNestedMessage(childMessageDefinition);
        addMemberFieldInMessageBuilder(nestedMessageName);
    }

    private void addMemberFieldInMessageBuilder(String fieldTypeOrNestedMessageName) {
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                fieldTypeOrNestedMessageName, getCurrentFieldName(), getCurrentFieldNumber());
        getMessageBuilder().addField(messageField);
    }
}
