package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_MAP_AS_UNION_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_TABLE_AS_UNION_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * ArrayMessageType.
 */
public class ArrayMessageType extends MessageType {
    private final MessageType parentMessageType;

    public ArrayMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        this(ballerinaType, messageBuilder, messageGenerator, null);
    }

    private ArrayMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                             BallerinaStructuredTypeMessageGenerator messageGenerator, MessageType parentMessageType) {
        super(ballerinaType, messageBuilder, messageGenerator);
        this.parentMessageType = parentMessageType;
    }

    public static ArrayMessageType withParentMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                                                         BallerinaStructuredTypeMessageGenerator messageGenerator,
                                                         MessageType parentMessageType) {
        return new ArrayMessageType(ballerinaType, messageBuilder, messageGenerator, parentMessageType);
    }


    @Override
    public void setCurrentFieldName(String fieldName) {
        if (super.getCurrentFieldName() == null) {
            super.setCurrentFieldName(fieldName);
        }
    }

    @Override
    public void setIntField(IntegerType integerType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(
                integerType);
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        String fieldName = getCurrentFieldName();
        if (parentMessageType instanceof UnionMessageType) {
            // Field names and nested message names are prefixed with ballerina type to avoid name collision
            fieldName = byteType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        }
        // Use optional label instead of repeated label, protobuf supports bytes instead of byte
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                getCurrentFieldNumber());
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setFloatField(FloatType floatType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(
                floatType);
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder nestedMessageBuilder = generateDecimalMessageDefinition();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(
                decimalType);
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setStringField(StringType stringType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(
                stringType);
        getMessageBuilder().addField(messageField);
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        ProtobufMessageFieldBuilder messageField = generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(
                booleanType);
        getMessageBuilder().addField(messageField);
    }

    private ProtobufMessageFieldBuilder generateMessageFieldWithRepeatedLabelForBallerinaPrimitiveType(Type type) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(type.getTag());
        String fieldName = getCurrentFieldName();
        if (parentMessageType instanceof UnionMessageType) {
            // Field names and nested message names are prefixed with ballerina type to avoid name collision
            fieldName = type.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        }
        return new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName, getCurrentFieldNumber());
    }

    @Override
    public void setRecordField(RecordType recordType) {
        // if not a referenced recordType use "RecordBuilder" as message name
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ? RECORD_BUILDER : recordType.getName();
        String fieldName = getCurrentFieldName();
        if (parentMessageType instanceof UnionMessageType) {
            String ballerinaType = recordType.getName();
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        } else if (isAnonymousBallerinaRecord(recordType) && (parentMessageType instanceof RecordMessageType
                || parentMessageType instanceof TupleMessageType)) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }

        ProtobufMessageBuilder messageBuilder = getMessageBuilder();

        // Check for cyclic reference in ballerina record
        boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);

            // context switch 1
            var current = getMessageGenerator().getMessageType();
            getMessageGenerator().setMessageType(
                    new RecordMessageType(recordType, nestedMessageBuilder, getMessageGenerator()));

            nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
            messageBuilder.addNestedMessage(nestedMessageBuilder);

            // context switch 2
            getMessageGenerator().setMessageType(current);
        }

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setMapField(MapType mapType) {
        String nestedMessageName = MAP_BUILDER;
        if (parentMessageType instanceof UnionMessageType) {
            // TODO: support array of map as union member
            throw createSerdesError(ARRAY_OF_MAP_AS_UNION_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setTableField(TableType tableType) {
        String nestedMessageName = TABLE_BUILDER;

        if (parentMessageType instanceof UnionMessageType) {
            // TODO: support array of table union member
            throw createSerdesError(ARRAY_OF_TABLE_AS_UNION_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();

        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TableMessageType(tableType, nestedMessageBuilder, getMessageGenerator()));

        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                getCurrentFieldName(), getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        String nestedMessageName = ARRAY_BUILDER_NAME;
        String fieldName = getCurrentFieldName();
        if (parentMessageType instanceof UnionMessageType) {
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());
            String ballerinaType = Utils.getElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            Type ballerinaType = Utils.getElementTypeOfBallerinaArray((ArrayType) getBallerinaType());
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());

            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            if (ballerinaType.getTag() == TypeTags.MAP_TAG || ballerinaType.getTag() == TypeTags.TABLE_TAG
                    || isAnonymousBallerinaRecord(ballerinaType)) {
                nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
            } else {
                nestedMessageName = ballerinaType.getName() + TYPE_SEPARATOR + nestedMessageName;
            }
        }

        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new ArrayMessageType(arrayType, nestedMessageBuilder, getMessageGenerator(), current));

        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String fieldName = getCurrentFieldName();
        String nestedMessageName = UNION_BUILDER_NAME;

        if (parentMessageType instanceof UnionMessageType) {
            String ballerinaType = Utils.getElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            String ballerinaType = Utils.getElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
        }
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

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = TUPLE_BUILDER;
        String fieldName = getCurrentFieldName();
        if (parentMessageType instanceof UnionMessageType) {
            String ballerinaType = tupleType.getName();
            if (ballerinaType.equals(EMPTY_STRING)) {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(tupleType), SERDES_ERROR);
            }
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + TUPLE_BUILDER;
            fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();

        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                fieldName, getCurrentFieldNumber());
        messageBuilder.addField(messageField);
    }
}
