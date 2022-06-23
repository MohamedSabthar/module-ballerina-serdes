package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * ArrayElementTypeHandler handle schema generation of ballerina array types.
 */
public class ArrayElementTypeHandler implements TypeHandler {

    private final BallerinaStructureTypeContext structureTypeContext;
    private final ProtobufMessageBuilder messageBuilder;
    private final int dimension;
    private final int fieldNumber;
    private final boolean isRecordField;
    private final boolean isUnionMember;

    private String fieldName;


    public ArrayElementTypeHandler(BallerinaStructureTypeContext structureTypeContext,
                                   ProtobufMessageBuilder messageBuilder, String fieldName, int dimension,
                                   int fieldNumber, boolean isUnionMember, boolean isRecordField) {
        this.structureTypeContext = structureTypeContext;
        this.messageBuilder = messageBuilder;
        this.fieldName = fieldName;
        this.dimension = dimension;
        this.fieldNumber = fieldNumber;
        this.isRecordField = isRecordField;
        this.isUnionMember = isUnionMember;
    }

    @Override
    public ProtobufMessageBuilder setNullField(int fieldNumber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProtobufMessageBuilder setIntField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.INT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setByteField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setFloatField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.FLOAT_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setStringField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.STRING_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setBooleanField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BOOLEAN_TAG);
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setDecimalField(String fieldTypeName, int fieldNumber) {
        changePrimitiveFieldNameIfUnionMember(fieldTypeName);
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

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName,
                this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setUnionField(UnionType unionType, String fieldTypeName, int fieldNumber) {
        String nestedMessageName = UNION_BUILDER_NAME;

        if (isUnionMember) {
            nestedMessageName = fieldTypeName + TYPE_SEPARATOR + nestedMessageName;
            fieldName = fieldTypeName + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        }

        TypeHandler currentHandler = structureTypeContext.getTypeHandler();
        Type currentType = structureTypeContext.getType();

        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
        TypeHandler unionHandler = new UnionMemberTypeHandler(structureTypeContext, nestedMessageBuilder);
        structureTypeContext.setTypeHandler(unionHandler);
        structureTypeContext.setType(unionType);
        messageBuilder.addNestedMessage(structureTypeContext.schema());

        structureTypeContext.setTypeHandler(currentHandler);
        structureTypeContext.setType(currentType);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setArrayField(ArrayType nestedArrayType, String fieldTypeName, int fieldNumber) {
        String nestedMessageName = ARRAY_BUILDER_NAME;

        if (isUnionMember) {
            String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        } else if (isRecordField) {
            String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
        }

        TypeHandler currentHandler = structureTypeContext.getTypeHandler();
        Type currentType = structureTypeContext.getType();

        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
        TypeHandler arrayHandler = new ArrayElementTypeHandler(structureTypeContext, nestedMessageBuilder,
                ARRAY_FIELD_NAME, -1, 1, false, false);
        structureTypeContext.setTypeHandler(arrayHandler);
        structureTypeContext.setType(nestedArrayType);
        messageBuilder.addNestedMessage(structureTypeContext.schema());

        structureTypeContext.setTypeHandler(currentHandler);
        structureTypeContext.setType(currentType);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    @Override
    public ProtobufMessageBuilder setRecordField(RecordType recordType, String fieldTypeName, int fieldNumber) {
        String nestedMessageName = recordType.getName();

        if (isUnionMember) {
            String ballerinaType = recordType.getName();
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        }

        // Check for cyclic reference in ballerina record
        boolean isRecordVisited = structureTypeContext.isRecordVisited(nestedMessageName);
        if (!isRecordVisited) {
            structureTypeContext.addVisitedRecord(recordType.getName());

            TypeHandler currentHandler = structureTypeContext.getTypeHandler();
            Type currentType = structureTypeContext.getType();

            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
            TypeHandler recordHandler = new RecordFieldTypeHandler(structureTypeContext, nestedMessageBuilder);
            structureTypeContext.setTypeHandler(recordHandler);
            structureTypeContext.setType(recordType);
            messageBuilder.addNestedMessage(structureTypeContext.schema());

            structureTypeContext.setTypeHandler(currentHandler);
            structureTypeContext.setType(currentType);
        }

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, this.fieldNumber);
        messageBuilder.addField(messageField);
        return messageBuilder;
    }

    private void changePrimitiveFieldNameIfUnionMember(String fieldTypeName) {
        if (isUnionMember) {
            // Field names and nested message names are prefixed with ballerina type to avoid name collision
            fieldName = fieldTypeName + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        }
    }
}
