package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * UnionMemberTypeHandler handle schema generation of ballerina union types.
 */
public class UnionMemberTypeHandler implements TypeHandler {

    private final BallerinaStructureTypeContext structureTypeContext;
    private ProtobufMessageBuilder messageBuilder;

    public UnionMemberTypeHandler(BallerinaStructureTypeContext structureTypeContext,
                                  ProtobufMessageBuilder messageBuilder) {
        this.structureTypeContext = structureTypeContext;
        this.messageBuilder = messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setNullField(int fieldNumber) {
        ProtobufMessageFieldBuilder nilField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BOOL, NULL_FIELD_NAME,
                fieldNumber);
        messageBuilder.addField(nilField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setIntField(String fieldName, int fieldNumber) {
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.INT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setByteField(String fieldName, int fieldNumber) {
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setFloatField(String fieldName, int fieldNumber) {
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.FLOAT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setStringField(String fieldName, int fieldNumber) {
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.STRING_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setBooleanField(String fieldName, int fieldNumber) {
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BOOLEAN_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setDecimalField(String fieldName, int fieldNumber) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(DECIMAL_VALUE);

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, 1);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                2);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, 3);

        nestedMessageBuilder.addField(scaleField);
        nestedMessageBuilder.addField(precisionField);
        nestedMessageBuilder.addField(valueField);

        messageBuilder.addNestedMessage(nestedMessageBuilder);


        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.DECIMAL_TAG);
        fieldName = fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setUnionField(UnionType unionType, String fieldName, int fieldNumber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProtobufMessageBuilder setArrayField(ArrayType arrayType, String discard, int fieldNumber) {
        int dimention = Utils.getDimensions(arrayType);
        String fieldName = ARRAY_FIELD_NAME + SEPARATOR + dimention;
        boolean isUnionMember = true;
        boolean isRecordField = false;

        TypeHandler currentHandler = structureTypeContext.getTypeHandler();
        Type currentType = structureTypeContext.getType();

        TypeHandler arrayHandler = new ArrayElementTypeHandler(structureTypeContext, messageBuilder, fieldName,
                dimention, fieldNumber, isUnionMember, isRecordField);
        structureTypeContext.setTypeHandler(arrayHandler);
        structureTypeContext.setType(arrayType);
        messageBuilder = structureTypeContext.schema();

        structureTypeContext.setTypeHandler(currentHandler);
        structureTypeContext.setType(currentType);

        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setRecordField(RecordType recordType, String discard, int fieldNumber) {
        String nestedMessageName = recordType.getName();

        // Check for cyclic reference in ballerina record
        boolean isRecordVisited = structureTypeContext.isRecordVisited(nestedMessageName);
        if (!isRecordVisited) {
            structureTypeContext.addVisitedRecord(nestedMessageName);
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);

            TypeHandler currentHandler = structureTypeContext.getTypeHandler();
            Type currentType = structureTypeContext.getType();

            TypeHandler recordHandler = new RecordFieldTypeHandler(structureTypeContext, nestedMessageBuilder);
            structureTypeContext.setTypeHandler(recordHandler);
            structureTypeContext.setType(recordType);
            messageBuilder.addNestedMessage(structureTypeContext.schema());

            structureTypeContext.setTypeHandler(currentHandler);
            structureTypeContext.setType(currentType);
        }

        String fieldName = recordType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                fieldName, fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }
}
