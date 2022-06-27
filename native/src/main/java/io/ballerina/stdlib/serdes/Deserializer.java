/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.serdes;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collection;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.DESERIALIZATION_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Deserializer class to generate Ballerina value from byte array.
 */
public class Deserializer {

    /**
     * Creates an anydata object from a byte array after deserializing.
     *
     * @param des            Deserializer object.
     * @param encodedMessage Byte array corresponding to encoded data.
     * @param dataType       Data type of the encoded value.
     * @return anydata object.
     */
    @SuppressWarnings("unused")
    public static Object deserialize(BObject des, BArray encodedMessage, BTypedesc dataType) {
        try {
            Descriptor messageDescriptor = (Descriptor) des.getNativeData(SCHEMA_NAME);
            DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, encodedMessage.getBytes());
            BTypedesc bTypedesc = (BTypedesc) des.get(BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
            return dynamicMessageToBallerinaType(message, bTypedesc.getDescribingType());
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (Exception e) {
            return createSerdesError(DESERIALIZATION_ERROR_MESSAGE + e.getMessage(), SERDES_ERROR);
        }
    }

    private static Object dynamicMessageToBallerinaType(DynamicMessage dynamicMessage, Type ballerinaType) {
        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                Descriptor messageDescriptor = dynamicMessage.getDescriptorForType();
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(ATOMIC_FIELD_NAME);
                Object messageFieldValue = dynamicMessage.getField(fieldDescriptor);
                return getPrimitiveTypeValueFromMessage(messageFieldValue);
            }

            case TypeTags.DECIMAL_TAG: {
                return getDecimalPrimitiveTypeValueFromMessage(dynamicMessage);
            }

            case TypeTags.UNION_TAG: {
                return getUnionTypeValueFromMessage(dynamicMessage, referredType);
            }

            case TypeTags.ARRAY_TAG: {
                Type elementType = ((ArrayType) referredType).getElementType();
                Descriptor messageDescriptor = dynamicMessage.getDescriptorForType();
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(ARRAY_FIELD_NAME);
                Object messageFieldValue = dynamicMessage.getField(fieldDescriptor);
                return getArrayTypeValueFromMessage(messageFieldValue, elementType, messageDescriptor);
            }

            case TypeTags.RECORD_TYPE_TAG: {
                return getRecordTypeValueFromMessage(dynamicMessage, (RecordType) referredType);
            }

            case TypeTags.MAP_TAG: {
                return getMapTypeValueFromMessage(dynamicMessage, (MapType) referredType);
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }

    private static Object getDecimalPrimitiveTypeValueFromMessage(DynamicMessage decimalMessage) {
        Descriptor decimalSchema = decimalMessage.getDescriptorForType();

        FieldDescriptor valueField = decimalSchema.findFieldByName(VALUE);
        FieldDescriptor scaleField = decimalSchema.findFieldByName(SCALE);
        FieldDescriptor precisionField = decimalSchema.findFieldByName(PRECISION);

        BigInteger value = new BigInteger(((ByteString) decimalMessage.getField(valueField)).toByteArray());
        Integer scale = (Integer) decimalMessage.getField(scaleField);
        MathContext precision = new MathContext((int) decimalMessage.getField(precisionField));

        BigDecimal bigDecimal = new BigDecimal(value, scale, precision);
        return ValueCreator.createDecimalValue(bigDecimal);
    }

    private static Object getPrimitiveTypeValueFromMessage(Object value) {
        if (value instanceof DynamicMessage) {
            DynamicMessage decimalMessage = ((DynamicMessage) value);
            return getDecimalPrimitiveTypeValueFromMessage(decimalMessage);
        }

        if (value instanceof ByteString) {
            return ((ByteString) value).byteAt(0);
        }

        if (value instanceof String) {
            return StringUtils.fromString((String) value);
        }

        return value;
    }

    private static Object getUnionTypeValueFromMessage(DynamicMessage dynamicMessage, Type type) {
        Descriptor messageDescriptor = dynamicMessage.getDescriptorForType();

        for (Map.Entry<FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            Object value = entry.getValue();
            FieldDescriptor fieldDescriptor = entry.getKey();

            // Handle null value
            if (fieldDescriptor.getName().equals(NULL_FIELD_NAME) && (boolean) value) {
                return null;
            }

            // Handle array values
            if (fieldDescriptor.isRepeated()) {
                String fieldName = fieldDescriptor.getName();
                String[] tokens = fieldName.split(TYPE_SEPARATOR);
                String ballerinaTypeName = tokens[0];
                int dimention = Integer.parseInt(tokens[1].split(SEPARATOR)[1]);
                ArrayType arrayType = getBallerinaArrayTypeFromUnion((UnionType) type, ballerinaTypeName, dimention);
                return getArrayTypeValueFromMessage(value, arrayType.getElementType(), messageDescriptor, dimention,
                        ballerinaTypeName);
            } else if (value instanceof ByteString && fieldDescriptor.getName().contains(ARRAY_FIELD_NAME)) {
                // Handle byte array values
                ByteString byteString = (ByteString) value;
                return ValueCreator.createArrayValue(byteString.toByteArray());
            } else if (value instanceof DynamicMessage && !fieldDescriptor.getMessageType().getName()
                    .contains(DECIMAL_VALUE)) {
                // Handle record values
                String fieldName = fieldDescriptor.getName();
                String[] tokens = fieldName.split(TYPE_SEPARATOR);
                String ballerinaType = tokens[0];
                RecordType recordType = getBallerinaRecordTypeFromUnion((UnionType) type, ballerinaType);
                return getRecordTypeValueFromMessage((DynamicMessage) value, recordType);
            } else {
                // Handle primitive values
                return getPrimitiveTypeValueFromMessage(value);
            }
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
    }

    private static Object getArrayTypeValueFromMessage(Object value, Type elementType, Descriptor messageDescriptor) {
        return getArrayTypeValueFromMessage(value, elementType, messageDescriptor, -1, null);
    }

    private static Object getArrayTypeValueFromMessage(Object value, Type elementType, Descriptor messageDescriptor,
                                                       int dimensions,
                                                       String ballerinaTypeNamePrefixOfUnionMemberOrRecordField) {
        // Handle byte array value
        if (value instanceof ByteString) {
            ByteString byteString = (ByteString) value;
            return ValueCreator.createArrayValue(byteString.toByteArray());
        }

        Collection<?> collection = ((Collection<?>) value);
        Type referredElementType = TypeUtils.getReferredType(elementType);
        BArray bArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(referredElementType));

        for (Object element : collection) {
            switch (referredElementType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    bArray.append(element);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    BDecimal decimal = (BDecimal) getDecimalPrimitiveTypeValueFromMessage((DynamicMessage) element);
                    bArray.append(decimal);
                    break;
                }

                case TypeTags.STRING_TAG: {
                    BString bString = StringUtils.fromString((String) element);
                    bArray.append(bString);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    DynamicMessage nestedDynamicMessage = (DynamicMessage) element;
                    Object unionValue = getUnionTypeValueFromMessage(nestedDynamicMessage, referredElementType);
                    bArray.append(unionValue);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) referredElementType;
                    String typeName = ARRAY_BUILDER_NAME;

                    if (ballerinaTypeNamePrefixOfUnionMemberOrRecordField != null) {
                        typeName = ARRAY_BUILDER_NAME + SEPARATOR + (dimensions - 1);
                        typeName = ballerinaTypeNamePrefixOfUnionMemberOrRecordField + TYPE_SEPARATOR + typeName;
                    }

                    Descriptor nestedSchema = messageDescriptor.findNestedTypeByName(typeName);
                    DynamicMessage nestedDynamicMessage = (DynamicMessage) element;
                    FieldDescriptor fieldDescriptor = nestedSchema.findFieldByName(ARRAY_FIELD_NAME);
                    Object nestedArrayContent = nestedDynamicMessage.getField(fieldDescriptor);
                    BArray nestedArray = (BArray) getArrayTypeValueFromMessage(nestedArrayContent,
                            arrayType.getElementType(), nestedSchema);
                    bArray.append(nestedArray);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType recordType = (RecordType) referredElementType;
                    Object record = getRecordTypeValueFromMessage((DynamicMessage) element, recordType);
                    bArray.append(record);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    MapType mapType = (MapType) referredElementType;
                    Object map = getMapTypeValueFromMessage((DynamicMessage) element, mapType);
                    bArray.append(map);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredElementType.getName(), SERDES_ERROR);
            }
        }
        return bArray;
    }

    private static Object getRecordTypeValueFromMessage(DynamicMessage dynamicMessage, RecordType recordType) {
        // getEmptyValue method is used to set false value to boolean fields in the ballerina record
        // protobuf doesn't serialize false value in the protobuf message
        BMap<BString, Object> record = recordType.getEmptyValue();

        FieldDescriptor fieldDescriptor;
        Object ballerinaValue;

        for (Map.Entry<FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            fieldDescriptor = entry.getKey();
            Object value = entry.getValue();
            String entryFieldName = fieldDescriptor.getName();
            Type entryFieldType = recordType.getFields().get(entryFieldName).getFieldType();
            Type referredEntryFieldType = TypeUtils.getReferredType(entryFieldType);
            switch (referredEntryFieldType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    ballerinaValue = getPrimitiveTypeValueFromMessage(value);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    ballerinaValue = getDecimalPrimitiveTypeValueFromMessage((DynamicMessage) value);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    ballerinaValue = getUnionTypeValueFromMessage((DynamicMessage) value, referredEntryFieldType);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) referredEntryFieldType;
                    Descriptor recordSchema = fieldDescriptor.getContainingType();

                    String ballerinaTypeName = Utils.getElementTypeOfBallerinaArray(arrayType);
                    int dimention = Utils.getDimensions(arrayType);

                    ballerinaValue = getArrayTypeValueFromMessage(value, arrayType.getElementType(), recordSchema,
                            dimention, ballerinaTypeName);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    Object recordMessage = dynamicMessage.getField(fieldDescriptor);
                    ballerinaValue = getRecordTypeValueFromMessage((DynamicMessage) recordMessage,
                            (RecordType) referredEntryFieldType);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    Object mapMessage = dynamicMessage.getField(fieldDescriptor);
                    ballerinaValue = getMapTypeValueFromMessage((DynamicMessage) mapMessage,
                            (MapType) referredEntryFieldType);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredEntryFieldType.getName(), SERDES_ERROR);
            }
            record.put(StringUtils.fromString(entryFieldName), ballerinaValue);
        }
        return record;
    }


    private static Object getMapTypeValueFromMessage(DynamicMessage dynamicMessage, MapType mapType) {
        BMap<BString, Object> ballerinaMap = ValueCreator.createMapValue(mapType);

        Type constrainedType = mapType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        FieldDescriptor mapFieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByName(MAP_FIELD);
        Collection<?> mapEntries = (Collection<?>) dynamicMessage.getField(mapFieldDescriptor);

        Object ballerinaValue;

        for (Object mapEntry : mapEntries) {
            DynamicMessage mapEntryMessage = (DynamicMessage) mapEntry;
            Descriptor mapEntryMessageDescriptor = mapEntryMessage.getDescriptorForType();
            FieldDescriptor keyFieldDescriptor = mapEntryMessageDescriptor.findFieldByName(KEY_NAME);
            FieldDescriptor valueFieldDescriptor = mapEntryMessageDescriptor.findFieldByName(VALUE_NAME);

            String key = (String) mapEntryMessage.getField(keyFieldDescriptor);
            Object value = mapEntryMessage.getField(valueFieldDescriptor);

            switch (referredConstrainedType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    ballerinaValue = getPrimitiveTypeValueFromMessage(value);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    ballerinaValue = getDecimalPrimitiveTypeValueFromMessage((DynamicMessage) value);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    ballerinaValue = getUnionTypeValueFromMessage((DynamicMessage) value, referredConstrainedType);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) referredConstrainedType;
                    Descriptor recordSchema = valueFieldDescriptor.getContainingType();

                    String ballerinaTypeName = Utils.getElementTypeOfBallerinaArray(arrayType);
                    int dimention = Utils.getDimensions(arrayType);

                    ballerinaValue = getArrayTypeValueFromMessage(value, arrayType.getElementType(), recordSchema,
                            dimention, ballerinaTypeName);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    ballerinaValue = getRecordTypeValueFromMessage((DynamicMessage) value,
                            (RecordType) referredConstrainedType);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    ballerinaValue = getMapTypeValueFromMessage((DynamicMessage) value,
                            (MapType) referredConstrainedType);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredConstrainedType.getName(), SERDES_ERROR);
            }
            ballerinaMap.put(StringUtils.fromString(key), ballerinaValue);
        }
        return ballerinaMap;
    }

    private static RecordType getBallerinaRecordTypeFromUnion(UnionType unionType, String targetBallerinaTypeName) {
        RecordType targetRecordType = null;

        for (Type memberType : unionType.getMemberTypes()) {
            memberType = TypeUtils.getReferredType(memberType);
            if (memberType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                String recordType = memberType.getName();
                if (recordType.equals(targetBallerinaTypeName)) {
                    targetRecordType = (RecordType) memberType;
                    break;
                }
            }
        }
        return targetRecordType;
    }

    private static ArrayType getBallerinaArrayTypeFromUnion(UnionType unionType, String targetBallerinaTypeName,
                                                            int dimention) {
        ArrayType targetArrayType = null;

        for (Type memberType : unionType.getMemberTypes()) {
            memberType = TypeUtils.getReferredType(memberType);
            if (memberType.getTag() == TypeTags.ARRAY_TAG) {
                String arrayBasicType = Utils.getElementTypeOfBallerinaArray((ArrayType) memberType);
                int arrayDimention = Utils.getDimensions((ArrayType) memberType);
                if (arrayDimention == dimention && arrayBasicType.equals(targetBallerinaTypeName)) {
                    targetArrayType = (ArrayType) memberType;
                    break;
                }
            }
        }
        return targetArrayType;
    }
}
