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
import com.google.protobuf.InvalidProtocolBufferException;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
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
            Descriptor schema = (Descriptor) des.getNativeData(Constants.SCHEMA_NAME);
            DynamicMessage message = DynamicMessage.parseFrom(schema, encodedMessage.getBytes());
            BTypedesc ballerinaSchemaType = (BTypedesc) des.get(Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
            return dynamicMessageToBallerinaType(message, ballerinaSchemaType.getDescribingType());
        } catch (BError e) {
            return e;
        } catch (InvalidProtocolBufferException e) {
            return createSerdesError(Constants.DESERIALIZATION_ERROR_MESSAGE + e.getMessage(), SERDES_ERROR);
        }
    }

    private static Object dynamicMessageToBallerinaType(DynamicMessage dynamicMessage, Type type) {

        FieldDescriptor fieldDescriptor;
        Descriptor schema = dynamicMessage.getDescriptorForType();

        switch (type.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String atomicFieldName = Constants.ATOMIC_FIELD_NAME;
                fieldDescriptor = schema.findFieldByName(atomicFieldName);
                Object value = dynamicMessage.getField(fieldDescriptor);
                return getPrimitiveTypeValueFromMessage(value);
            }

            case TypeTags.DECIMAL_TAG: {
                return getDecimalPrimitiveTypeValueFromMessage(dynamicMessage);
            }

            case TypeTags.UNION_TAG: {
                return getUnionTypeValueFromMessage(dynamicMessage, type);
            }

            case TypeTags.ARRAY_TAG: {
                int dimensions = Utils.getDimensions((ArrayType) type);
                String fieldName = Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + dimensions;

                Type elementType = ((ArrayType) type).getElementType();
                fieldDescriptor = schema.findFieldByName(fieldName);
                schema = fieldDescriptor.getContainingType();
                Object value = dynamicMessage.getField(fieldDescriptor);
                return getArrayTypeValueFromMessage(value, elementType, schema, dimensions);
            }

            case TypeTags.RECORD_TYPE_TAG: {
                return getRecordTypeValueFromMessage(dynamicMessage, (RecordType) type);
            }

            default:
                throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    private static Object getDecimalPrimitiveTypeValueFromMessage(DynamicMessage decimalMessage) {

        Descriptor decimalSchema = decimalMessage.getDescriptorForType();
        var valueField = decimalSchema.findFieldByName(Constants.VALUE);
        var scaleField = decimalSchema.findFieldByName(Constants.SCALE);
        var precisionField = decimalSchema.findFieldByName(Constants.PRECISION);

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

        Descriptor schema = dynamicMessage.getDescriptorForType();

        for (var entry : dynamicMessage.getAllFields().entrySet()) {
            Object value = entry.getValue();
            FieldDescriptor fieldDescriptor = entry.getKey();

            if (fieldDescriptor.getName().equals(Constants.NULL_FIELD_NAME) && (boolean) value) {
                return null;
            }

            if (fieldDescriptor.getType().name().equals(Constants.MESSAGE)
                    && fieldDescriptor.getMessageType().getName().contains(Constants.RECORD_BUILDER_NAME)) {
                String fieldName = fieldDescriptor.getName();
                String[] tokens = fieldName.split(Constants.TYPE_SEPARATOR);
                String ballerinaType = tokens[0];
                RecordType recordType = getBallerinaRecordTypeFromUnion((UnionType) type, ballerinaType);
                return getRecordTypeValueFromMessage((DynamicMessage) value, recordType);
            }

            if (fieldDescriptor.isRepeated()) {
                String fieldName = fieldDescriptor.getName();
                String[] tokens = fieldName.split(Constants.TYPE_SEPARATOR);
                String ballerinaType = tokens[0];
                int dimention = Integer.parseInt(tokens[1].split(Constants.SEPARATOR)[1]);
                ArrayType arrayType = getBallerinaArrayTypeFromUnion((UnionType) type, ballerinaType, dimention);
                return getArrayTypeValueFromMessage(
                        value, arrayType.getElementType(), schema, dimention, ballerinaType);

            } else if (value instanceof ByteString && fieldDescriptor.getName().contains(Constants.ARRAY_FIELD_NAME)) {
                ByteString byteString = (ByteString) value;
                return ValueCreator.createArrayValue(byteString.toByteArray());
            } else {
                return getPrimitiveTypeValueFromMessage(value);
            }
        }
        return null;
    }

    private static Object getArrayTypeValueFromMessage(Object value, Type type, Descriptor schema, int dimensions) {
        return getArrayTypeValueFromMessage(value, type, schema, dimensions, null);
    }

    private static Object getArrayTypeValueFromMessage(
            Object value, Type elementType, Descriptor schema, int dimensions, String ballerinaTypePrefixOfUnionField) {

        if (value instanceof ByteString) {
            ByteString byteString = (ByteString) value;
            return ValueCreator.createArrayValue(byteString.toByteArray());
        }

        Collection<?> collection = ((Collection<?>) value);
        BArray bArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(elementType));

        for (Object element : collection) {
            switch (elementType.getTag()) {
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
                    Object unionValue = getUnionTypeValueFromMessage(nestedDynamicMessage, elementType);
                    bArray.append(unionValue);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) elementType;
                    String fieldName = Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + (dimensions - 1);
                    String typeName = Constants.ARRAY_BUILDER_NAME + Constants.SEPARATOR + (dimensions - 1);

                    if (ballerinaTypePrefixOfUnionField != null) {
                        typeName = ballerinaTypePrefixOfUnionField + Constants.TYPE_SEPARATOR + typeName;
                    }

                    Descriptor nestedSchema = schema.findNestedTypeByName(typeName);
                    DynamicMessage nestedDynamicMessage = (DynamicMessage) element;
                    FieldDescriptor fieldDescriptor = nestedSchema.findFieldByName(fieldName);
                    Object nestedArrayContent = nestedDynamicMessage.getField(fieldDescriptor);
                    BArray nestedArray = (BArray) getArrayTypeValueFromMessage(nestedArrayContent,
                            arrayType.getElementType(), nestedSchema, dimensions - 1);
                    bArray.append(nestedArray);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType recordType = (RecordType) elementType;
                    Object record = getRecordTypeValueFromMessage((DynamicMessage) element, recordType);
                    bArray.append(record);
                    break;
                }
            }
        }
        return bArray;
    }

    private static Object getRecordTypeValueFromMessage(DynamicMessage dynamicMessage, RecordType recordType) {

        BMap<BString, Object> record = ValueCreator.createRecordValue(recordType);

        FieldDescriptor fieldDescriptor;
        Object ballerinaValue;

        for (var entry : dynamicMessage.getAllFields().entrySet()) {
            fieldDescriptor = entry.getKey();
            Object value = entry.getValue();
            String entryFieldName = fieldDescriptor.getName();
            Type entryFieldType = recordType.getFields().get(entryFieldName).getFieldType();

            switch (entryFieldType.getTag()) {
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
                    ballerinaValue = getUnionTypeValueFromMessage((DynamicMessage) value, entryFieldType);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) entryFieldType;
                    int dimensions = Utils.getDimensions(arrayType);
                    Descriptor recordSchema = fieldDescriptor.getContainingType();
                    ballerinaValue = getArrayTypeValueFromMessage(value, arrayType.getElementType(),
                            recordSchema, dimensions);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    Object recordMessage = dynamicMessage.getField(fieldDescriptor);
                    ballerinaValue = getRecordTypeValueFromMessage((DynamicMessage) recordMessage,
                            (RecordType) entryFieldType);
                    break;
                }

                default:
                    throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + entryFieldType.getName(),
                            SERDES_ERROR);

            }
            record.put(StringUtils.fromString(entryFieldName), ballerinaValue);
        }
        return record;
    }

    private static RecordType getBallerinaRecordTypeFromUnion(UnionType unionType, String ballerinaType) {
        RecordType type = null;

        for (var memberTypes : unionType.getMemberTypes()) {
            if (memberTypes instanceof RecordType) {
                String recordType = memberTypes.getName();
                if (recordType.equals(ballerinaType)) {
                    type = (RecordType) memberTypes;
                    break;
                }
            }
        }
        return type;
    }

    private static ArrayType getBallerinaArrayTypeFromUnion(UnionType unionType, String ballerinaType, int dimention) {
        ArrayType type = null;

        for (var memberTypes : unionType.getMemberTypes()) {
            if (memberTypes instanceof ArrayType) {
                String arrayBasicType = Utils.getElementTypeOfBallerinaArray((ArrayType) memberTypes);
                int arrayDimention = Utils.getDimensions((ArrayType) memberTypes);
                if (arrayDimention == dimention && arrayBasicType.equals(ballerinaType)) {
                    type = (ArrayType) memberTypes;
                    break;
                }
            }
        }
        return type;
    }
}
