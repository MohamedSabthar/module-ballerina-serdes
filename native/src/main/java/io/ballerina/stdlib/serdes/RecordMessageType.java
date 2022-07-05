package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
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
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Utils.isNonReferencedRecordType;

/**
 * RecordMessageType.
 */
public class RecordMessageType extends MessageType {

    public RecordMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                             BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    private ProtobufMessageFieldBuilder generateMessageField(int typeTag) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(typeTag);
        return new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, getCurrentFieldName(),
                getCurrentFieldNumber());
    }

    @Override
    void setIntField(IntegerType integerType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.INT_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setByteField(ByteType byteType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.BYTE_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setFloatField(FloatType floatType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.FLOAT_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setDecimalField(DecimalType decimalType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.DECIMAL_TAG);
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, 1);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                2);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, 3);

        nestedMessageBuilder.addField(scaleField);
        nestedMessageBuilder.addField(precisionField);
        nestedMessageBuilder.addField(valueField);

        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.DECIMAL_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setStringField(StringType stringType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.STRING_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setBooleanField(BooleanType booleanType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.BOOLEAN_TAG);
        getMessageBuilder().addField(messageField);
    }

    @Override
    void setNullField(NullType nullType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setRecordField(RecordType recordType) {
        String nestedMessageName = isNonReferencedRecordType(recordType) ?
                getCurrentFieldName() + TYPE_SEPARATOR + RECORD_BUILDER : recordType.getName();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        // Check for cyclic reference in ballerina record
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

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    void setMapField(MapType mapType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + MAP_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    void setTableField(TableType tableType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TABLE_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TableMessageType(tableType, nestedMessageBuilder, getMessageGenerator()));

        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new ArrayMessageType(arrayType, messageBuilder, getMessageGenerator(), current));
        getMessageGenerator().getMessageType().setCurrentFieldName(getCurrentFieldName());
        getMessageGenerator().getMessageType().setCurrentFieldNumber(getCurrentFieldNumber());
        getMessageGenerator().generateMessageDefinition();

        // context switch 2
        getMessageGenerator().setMessageType(current);
    }

    @Override
    void setUnionField(UnionType unionType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new UnionMessageType(unionType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    void setTupleField(TupleType tupleType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TUPLE_BUILDER;
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
