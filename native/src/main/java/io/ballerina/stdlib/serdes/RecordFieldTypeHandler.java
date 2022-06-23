package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * RecordFieldTypeHandler handle schema generation of ballerina record types.
 */
public class RecordFieldTypeHandler implements TypeHandler {

    private final BallerinaStructureTypeContext structureTypeContext;
    private ProtobufMessageBuilder messageBuilder;

    public RecordFieldTypeHandler(BallerinaStructureTypeContext structureTypeContext,
                                  ProtobufMessageBuilder messageBuilder) {
        this.structureTypeContext = structureTypeContext;
        this.messageBuilder = messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setNullField(int fieldNumber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProtobufMessageBuilder setIntField(String fieldName, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.INT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setByteField(String fieldName, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setFloatField(String fieldName, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.FLOAT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setStringField(String fieldName, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.STRING_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setBooleanField(String fieldName, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BOOLEAN_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setDecimalField(String fieldName, int fieldNumber) {
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

        messageBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setUnionField(UnionType unionType, String fieldName, int fieldNumber) {
        String nestedMessageName = fieldName + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);

        TypeHandler currentHandler = structureTypeContext.getTypeHandler();
        Type currentType = structureTypeContext.getType();

        TypeHandler unionHandler = new UnionMemberTypeHandler(structureTypeContext, nestedMessageBuilder);
        structureTypeContext.setTypeHandler(unionHandler);
        structureTypeContext.setType(unionType);
        messageBuilder.addNestedMessage(structureTypeContext.schema());

        structureTypeContext.setTypeHandler(currentHandler);
        structureTypeContext.setType(currentType);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                fieldName, fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setArrayField(ArrayType arrayType, String fieldName, int fieldNumber) {
        int dimention = Utils.getDimensions(arrayType);
        boolean isRecordField = true;
        boolean isUnionField = false;

        TypeHandler currentHandler = structureTypeContext.getTypeHandler();
        Type currentType = structureTypeContext.getType();

        TypeHandler arrayHandler = new ArrayElementTypeHandler(structureTypeContext, messageBuilder, fieldName,
                dimention, fieldNumber, isUnionField, isRecordField);
        structureTypeContext.setTypeHandler(arrayHandler);
        structureTypeContext.setType(arrayType);
        messageBuilder = structureTypeContext.schema();

        structureTypeContext.setTypeHandler(currentHandler);
        structureTypeContext.setType(currentType);

        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setRecordField(RecordType nestedRecordType, String fieldName, int fieldNumber) {
        String nestedMessageName = nestedRecordType.getName();
        boolean isRecordVisited = structureTypeContext.isRecordVisited(nestedMessageName);

        // Check for cyclic reference in ballerina record
        if (!isRecordVisited) {
            structureTypeContext.addVisitedRecord(nestedRecordType.getName());

            TypeHandler currentHandler = structureTypeContext.getTypeHandler();
            Type currentType = structureTypeContext.getType();

            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
            TypeHandler recordHandler = new RecordFieldTypeHandler(structureTypeContext, nestedMessageBuilder);
            structureTypeContext.setTypeHandler(recordHandler);
            structureTypeContext.setType(nestedRecordType);
            messageBuilder.addNestedMessage(structureTypeContext.schema());

            structureTypeContext.setTypeHandler(currentHandler);
            structureTypeContext.setType(currentType);
        }

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                fieldName, fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }
}
