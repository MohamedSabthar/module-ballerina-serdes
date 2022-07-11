package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;

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
 * MessageFieldNameGenerator generate protobuf message field names for the given ballerina structured type.
 */
public class MessageFieldNameGenerator {

    private MessageFieldNameGenerator() {
    }

    public static List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList(Type ballerinaStructureType) {
        Type referredType = TypeUtils.getReferredType(ballerinaStructureType);
        switch (referredType.getTag()) {
            case TypeTags.RECORD_TYPE_TAG:
                return getRecordFieldNamesAndTypes((RecordType) referredType);
            case TypeTags.ARRAY_TAG:
                return getArrayElementNameAndTypeAsList((ArrayType) referredType);
            case TypeTags.UNION_TAG:
                return getUnionMemberNamesAndTypes((UnionType) referredType);
            case TypeTags.TUPLE_TAG:
                return getTupleElementNamesAndTypes((TupleType) referredType);
            case TypeTags.MAP_TAG:
                return getMapValueFieldNameAndConstraintTypeAsList((MapType) referredType);
            case TypeTags.TABLE_TAG:
                return getTableEntryFieldNameAndConstraintTypeAsList((TableType) referredType);
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }

    private static List<Map.Entry<String, Type>> getRecordFieldNamesAndTypes(RecordType recordType) {
        Map<String, Field> recordFields = recordType.getFields();
        return recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> Map.entry(field.getFieldName(), TypeUtils.getReferredType(field.getFieldType())))
                .collect(Collectors.toList());
    }

    private static List<Map.Entry<String, Type>> getArrayElementNameAndTypeAsList(ArrayType arrayType) {
        Type elementType = arrayType.getElementType();
        Type referredType = TypeUtils.getReferredType(elementType);
        return List.of(Map.entry(ARRAY_FIELD_NAME, referredType));
    }

    private static List<Map.Entry<String, Type>> getUnionMemberNamesAndTypes(UnionType unionType) {
        return unionType.getMemberTypes().stream().map(MessageFieldNameGenerator::mapUnionMemberToMapEntry)
                .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    }

    private static Map.Entry<String, Type> mapUnionMemberToMapEntry(Type type) {
        Type referredType = TypeUtils.getReferredType(type);
        String typeName = referredType.getName();

        if (referredType.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getArrayDimensions((ArrayType) referredType);
            typeName = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) referredType);
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

    private static List<Map.Entry<String, Type>> getTupleElementNamesAndTypes(TupleType tupleType) {
        AtomicInteger elementIndex = new AtomicInteger(0);
        return tupleType.getTupleTypes().stream()
                .map(type -> Map.entry(TUPLE_FIELD_NAME + SEPARATOR + (elementIndex.incrementAndGet()),
                        TypeUtils.getReferredType(type))).collect(Collectors.toList());
    }

    private static List<Map.Entry<String, Type>> getMapValueFieldNameAndConstraintTypeAsList(MapType mapType) {
        return List.of(Map.entry(VALUE_NAME, TypeUtils.getReferredType(mapType.getConstrainedType())));
    }

    private static List<Map.Entry<String, Type>> getTableEntryFieldNameAndConstraintTypeAsList(TableType tableType) {
        return List.of(Map.entry(TABLE_ENTRY, TypeUtils.getReferredType(tableType.getConstrainedType())));
    }
}
