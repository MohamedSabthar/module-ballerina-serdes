package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.Field;
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
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.MAP_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.TABLE_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * BallerinaStructuredTypeMessageGenerator.
 */
public class BallerinaStructuredTypeMessageGenerator {
    private MessageType messageType;

    public BallerinaStructuredTypeMessageGenerator(Type type, ProtobufMessageBuilder messageBuilder) {
        switch (type.getTag()) {
            case TypeTags.RECORD_TYPE_TAG:
                setMessageType(new RecordMessageType(type, messageBuilder, this));
                break;
            case TypeTags.ARRAY_TAG:
                setMessageType(new ArrayMessageType(type, messageBuilder, this));
                break;
            case TypeTags.TUPLE_TAG:
                setMessageType(new TupleMessageType(type, messageBuilder, this));
                break;
            case TypeTags.UNION_TAG:
                setMessageType(new UnionMessageType(type, messageBuilder, this));
                break;
            case TypeTags.MAP_TAG:
                setMessageType(new MapMessageType(type, messageBuilder, this));
                break;
            case TypeTags.TABLE_TAG:
                setMessageType(new TableMessageType(type, messageBuilder, this));
                break;
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    private List<Map.Entry<String, Type>> getRecordFieldNamesAndTypes(RecordType recordType) {
        Map<String, Field> recordFields = recordType.getFields();
        return recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> Map.entry(field.getFieldName(), TypeUtils.getReferredType(field.getFieldType())))
                .collect(Collectors.toList());
    }

    private List<Map.Entry<String, Type>> getArrayElementNameAndTypeAsList(ArrayType arrayType) {
        Type elementType = arrayType.getElementType();
        Type referredType = TypeUtils.getReferredType(elementType);
        return List.of(Map.entry(ARRAY_FIELD_NAME, referredType));
    }

    private List<Map.Entry<String, Type>> getUnionMemberNamesAndTypes(UnionType unionType) {
        return unionType.getMemberTypes().stream().map(this::mapUnionMemberToMapEntry)
                .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    }

    private Map.Entry<String, Type> mapUnionMemberToMapEntry(Type type) {
        Type referredType = TypeUtils.getReferredType(type);
        String typeName = referredType.getName();

        if (referredType.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getArrayDimensions((ArrayType) referredType);
            typeName = Utils.getElementTypeNameOfBallerinaArray((ArrayType) referredType);

            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention + TYPE_SEPARATOR
                    + UNION_FIELD_NAME;

            return Map.entry(key, referredType);
        }

        // Handle enum members
        if (referredType.getTag() == TypeTags.FINITE_TYPE_TAG
                && TypeUtils.getType(referredType.getEmptyValue()).getTag() == TypeTags.STRING_TAG) {
            return Map.entry(((BString) referredType.getEmptyValue()).getValue(), referredType);
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)
                || referredType.getTag() == TypeTags.RECORD_TYPE_TAG) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, referredType);
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, referredType);
        }

        if (referredType.getTag() == TypeTags.TUPLE_TAG) {
            if (!typeName.equals(EMPTY_STRING)) {
                String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                return Map.entry(key, referredType);
            } else {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(type), SERDES_ERROR);
            }
        }

        if (referredType.getTag() == TypeTags.MAP_TAG) {
            // TODO: support map member
            throw createSerdesError(MAP_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        if (referredType.getTag() == TypeTags.TABLE_TAG) {
            // TODO: support table member
            throw createSerdesError(TABLE_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
    }

    private List<Map.Entry<String, Type>> getTupleElementNamesAndTypes(TupleType tupleType) {
        AtomicInteger elementIndex = new AtomicInteger(0);
        return tupleType.getTupleTypes().stream()
                .map(type -> Map.entry(TUPLE_FIELD_NAME + SEPARATOR + (elementIndex.incrementAndGet()),
                        TypeUtils.getReferredType(type))).collect(Collectors.toList());
    }

    private List<Map.Entry<String, Type>> getMapValueFieldNameAndConstraintTypeAsList(MapType mapType) {
        return List.of(Map.entry(VALUE_NAME, TypeUtils.getReferredType(mapType.getConstrainedType())));
    }

    private List<Map.Entry<String, Type>> getMessageFieldNamesAndBallerinaTypes() {
        Type type = messageType.getBallerinaType();

        switch (type.getTag()) {
            case TypeTags.RECORD_TYPE_TAG:
                return getRecordFieldNamesAndTypes((RecordType) type);
            case TypeTags.ARRAY_TAG:
                return getArrayElementNameAndTypeAsList((ArrayType) type);
            case TypeTags.UNION_TAG:
                return getUnionMemberNamesAndTypes((UnionType) type);
            case TypeTags.TUPLE_TAG:
                return getTupleElementNamesAndTypes((TupleType) type);
            case TypeTags.MAP_TAG:
                return getMapValueFieldNameAndConstraintTypeAsList((MapType) type);
            case TypeTags.TABLE_TAG:
                return getTableEntryFieldNameAndConstraintTypeAsList((TableType) type);
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    private List<Map.Entry<String, Type>> getTableEntryFieldNameAndConstraintTypeAsList(TableType tableType) {
        return List.of(Map.entry(TABLE_ENTRY, TypeUtils.getReferredType(tableType.getConstrainedType())));
    }

    public ProtobufMessageBuilder generateMessageDefinition() {

        List<Map.Entry<String, Type>> fieldNamesAndTypes = getMessageFieldNamesAndBallerinaTypes();

        for (Map.Entry<String, Type> entry : fieldNamesAndTypes) {
            String fieldEntryName = entry.getKey();
            Type fieldEntryType = entry.getValue();

            messageType.setCurrentFieldName(fieldEntryName);

            switch (fieldEntryType.getTag()) {
                case TypeTags.NULL_TAG:
                    messageType.setNullField((NullType) fieldEntryType);
                    break;
                case TypeTags.INT_TAG:
                    messageType.setIntField((IntegerType) fieldEntryType);
                    break;
                case TypeTags.BYTE_TAG:
                    messageType.setByteField((ByteType) fieldEntryType);
                    break;
                case TypeTags.FLOAT_TAG:
                    messageType.setFloatField((FloatType) fieldEntryType);
                    break;
                case TypeTags.STRING_TAG:
                    messageType.setStringField((StringType) fieldEntryType);
                    break;
                case TypeTags.BOOLEAN_TAG:
                    messageType.setBooleanField((BooleanType) fieldEntryType);
                    break;
                case TypeTags.DECIMAL_TAG:
                    messageType.setDecimalField((DecimalType) fieldEntryType);
                    break;
                case TypeTags.FINITE_TYPE_TAG:
                    messageType.setEnumField((FiniteType) fieldEntryType);
                    break;
                case TypeTags.UNION_TAG:
                    messageType.setUnionField((UnionType) fieldEntryType);
                    break;
                case TypeTags.ARRAY_TAG:
                    messageType.setArrayField((ArrayType) fieldEntryType);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    messageType.setRecordField((RecordType) fieldEntryType);
                    break;
                case TypeTags.TUPLE_TAG:
                    messageType.setTupleField((TupleType) fieldEntryType);
                    break;
                case TypeTags.TABLE_TAG:
                    messageType.setTableField((TableType) fieldEntryType);
                    break;
                case TypeTags.MAP_TAG:
                    messageType.setMapField((MapType) fieldEntryType);
                    break;
                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + fieldEntryType.getName(), SERDES_ERROR);
            }
            messageType.incrementFieldNumber();
        }
        return messageType.getMessageBuilder();
    }
}
