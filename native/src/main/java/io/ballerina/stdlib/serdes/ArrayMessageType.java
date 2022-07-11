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
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(integerType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        // Use optional label instead of repeated label, protobuf supports bytes instead of byte
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    @Override
    public void setFloatField(FloatType floatType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(floatType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder nestedMessageBuilder = generateDecimalMessageDefinition();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(decimalType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setStringField(StringType stringType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(stringType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(booleanType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        // if not a referenced recordType use "RecordBuilder" as message name
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ? RECORD_BUILDER : recordType.getName();
        if (isAnonymousBallerinaRecord(recordType) && (parentMessageType instanceof RecordMessageType
                || parentMessageType instanceof TupleMessageType)) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }
        addNestedMessageDefinitionInMessageBuilder(recordType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
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
        addNestedMessageDefinitionInMessageBuilder(mapType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
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

        addNestedMessageDefinitionInMessageBuilder(tableType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        String nestedMessageName = ARRAY_BUILDER_NAME;
        if (parentMessageType instanceof UnionMessageType) {
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());
            String ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            Type ballerinaType = Utils.getBaseElementTypeOfBallerinaArray((ArrayType) getBallerinaType());
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());

            nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            if (ballerinaType.getTag() == TypeTags.MAP_TAG || ballerinaType.getTag() == TypeTags.TABLE_TAG
                    || isAnonymousBallerinaRecord(ballerinaType)) {
                nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
            } else {
                nestedMessageName = ballerinaType.getName() + TYPE_SEPARATOR + nestedMessageName;
            }
        }

        addNestedMessageDefinitionInMessageBuilder(arrayType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String nestedMessageName = UNION_BUILDER_NAME;

        if (parentMessageType instanceof UnionMessageType) {
            String ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            String ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
        }
        addNestedMessageDefinitionInMessageBuilder(unionType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = TUPLE_BUILDER;
        if (parentMessageType instanceof UnionMessageType) {
            String ballerinaType = tupleType.getName();
            if (ballerinaType.equals(EMPTY_STRING)) {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(tupleType), SERDES_ERROR);
            }
            nestedMessageName = ballerinaType + TYPE_SEPARATOR + TUPLE_BUILDER;
        } else if (parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType) {
            nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + nestedMessageName;
        }
        addNestedMessageDefinitionInMessageBuilder(tupleType, nestedMessageName);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, nestedMessageName);
    }
}
