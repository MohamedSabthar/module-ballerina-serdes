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

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;
import static io.ballerina.stdlib.serdes.Utils.isNonReferencedRecordType;

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
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, STRING,
                getCurrentFieldName(), getCurrentFieldNumber());
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setNullField(NullType nullType) {
        ProtobufMessageFieldBuilder nilField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BOOL,
                getCurrentFieldName(), getCurrentFieldNumber());
        getMessageBuilder().addField(nilField);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = recordType.getName();

        if (isNonReferencedRecordType(recordType)) {
            throw createSerdesError(Utils.typeNotSupportedErrorMessage(recordType), SERDES_ERROR);
        }
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        // Check for cyclic reference in ballerina record
        boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);

            // context switch 1
            var current = getMessageGenerator().getMessageType();
            getMessageGenerator().setMessageType(
                    new RecordMessageType(recordType, nestedMessageBuilder, getMessageGenerator()));

            nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
            messageBuilder.addNestedMessage(nestedMessageBuilder);

            // context switch 2
            getMessageGenerator().setMessageType(current);
        }

        String fieldName = recordType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                fieldName, getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setMapField(MapType mapType) {
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + mapType.getName(), SERDES_ERROR);
    }

    @Override
    public void setTableField(TableType tableType) {
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + tableType.getName(), SERDES_ERROR);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new ArrayMessageType(arrayType, messageBuilder, getMessageGenerator(), current));
        int dimention = Utils.getArrayDimensions(arrayType);
        getMessageGenerator().getMessageType().setCurrentFieldName(ARRAY_FIELD_NAME + SEPARATOR + dimention);
        getMessageGenerator().getMessageType().setCurrentFieldNumber(getCurrentFieldNumber());
        getMessageGenerator().generateMessageDefinition();

        // context switch 2
        getMessageGenerator().setMessageType(current);
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

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }
}
