package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * BallerinaStructuredType provide the context to create proto message definition for some ballerina structured types.
 */
public class BallerinaStructureTypeContext {
    private Type type;
    private final HashMap<String, String> visitedRecords;
    private TypeHandler typeHandler;

    public BallerinaStructureTypeContext(ProtobufMessageBuilder messageBuilder, Type type) {
        this(messageBuilder, type, null);
    }

    public BallerinaStructureTypeContext(ProtobufMessageBuilder messageBuilder, Type type,
                                         HashMap<String, String> visitedRecords) {
        this.type = type;
        this.visitedRecords = Objects.requireNonNullElseGet(visitedRecords, HashMap::new);

        switch (type.getTag()) {
            case TypeTags.ARRAY_TAG:
                typeHandler = new ArrayElementTypeHandler(this, messageBuilder, ARRAY_FIELD_NAME, -1, 1, false, false);
                break;
            case TypeTags.UNION_TAG:
                typeHandler = new UnionMemberTypeHandler(this, messageBuilder);
                break;
            case TypeTags.RECORD_TYPE_TAG:
                addVisitedRecord(type.getName());
                typeHandler = new RecordFieldTypeHandler(this, messageBuilder);
                break;
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    public void setTypeHandler(TypeHandler typeHandler) {
        this.typeHandler = typeHandler;
    }

    public TypeHandler getTypeHandler() {
        return typeHandler;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public ProtobufMessageBuilder schema() {

        List<Object> fieldOrMemberOrElementEntries = getListOfEntries();
        ProtobufMessageBuilder messageBuilder = null;

        int fieldNumber = 1;

        for (Object entry : fieldOrMemberOrElementEntries) {
            String fieldEntryName;
            Type fieldEntryType;
            if (entry instanceof Type) {
                fieldEntryType = (Type) entry;
                fieldEntryName = fieldEntryType.getName();
            } else {
                Field field = (Field) entry;
                fieldEntryType = field.getFieldType();
                fieldEntryName = field.getFieldName();
            }

            switch (fieldEntryType.getTag()) {
                case TypeTags.NULL_TAG:
                    messageBuilder = typeHandler.setNullField(fieldNumber);
                    break;
                case TypeTags.INT_TAG:
                    messageBuilder = typeHandler.setIntField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.BYTE_TAG:
                    messageBuilder = typeHandler.setByteField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.FLOAT_TAG:
                    messageBuilder = typeHandler.setFloatField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.STRING_TAG:
                    messageBuilder = typeHandler.setStringField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.BOOLEAN_TAG:
                    messageBuilder = typeHandler.setBooleanField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.DECIMAL_TAG:
                    messageBuilder = typeHandler.setDecimalField(fieldEntryName, fieldNumber);
                    break;
                case TypeTags.UNION_TAG:
                    messageBuilder = typeHandler.setUnionField((UnionType) fieldEntryType, fieldEntryName, fieldNumber);
                    break;
                case TypeTags.ARRAY_TAG:
                    messageBuilder = typeHandler.setArrayField((ArrayType) fieldEntryType, fieldEntryName, fieldNumber);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    messageBuilder = typeHandler.setRecordField((RecordType) fieldEntryType, fieldEntryName,
                            fieldNumber);
                    break;

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + fieldEntryType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }

        return messageBuilder;
    }

    private List<Object> getListOfEntries() {
        if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            Map<String, Field> recordFields = ((RecordType) type).getFields();
            return recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName))
                    .collect(Collectors.toList());
        } else if (type.getTag() == TypeTags.UNION_TAG) {
            return ((UnionType) type).getMemberTypes().stream().map(this::mapUnionMemberToMapEntry)
                    .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());
        } else if (type.getTag() == TypeTags.ARRAY_TAG) {
            Type elementType = ((ArrayType) type).getElementType();
            return List.of(elementType);
        } else {
            throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }


    private Map.Entry<String, Type> mapUnionMemberToMapEntry(Type type) {
        String typeName = type.getName();

        if (type.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getDimensions((ArrayType) type);
            typeName = Utils.getElementTypeOfBallerinaArray((ArrayType) type);
            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention + TYPE_SEPARATOR
                    + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            String key = type.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, type);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
    }
    
    public boolean isRecordVisited(String recordName) {
        return visitedRecords.get(recordName) != null;
    }

    public void addVisitedRecord(String recordName) {
        visitedRecords.put(recordName, recordName);
    }
}
