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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;

import java.math.BigDecimal;

import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Serializer class to create a byte array for a value.
 */
public class Serializer {

    /**
     * Creates a BArray for given data after serializing.
     *
     * @param serializer Serializer object.
     * @param anydata    Data that is being serialized.
     * @return Byte array of the serialized value.
     */
    @SuppressWarnings("unused")
    public static Object serialize(BObject serializer, Object anydata) {
        BTypedesc dataType = (BTypedesc) serializer.get(Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
        Descriptor schema = (Descriptor) serializer.getNativeData(Constants.SCHEMA_NAME);
        DynamicMessage dynamicMessage;
        try {
            dynamicMessage = buildDynamicMessageFromType(anydata, schema, dataType.getDescribingType()).build();
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (IllegalArgumentException e) {
            String errorMessage = Constants.SERIALIZATION_ERROR_MESSAGE + Constants.TYPE_MISMATCH_ERROR_MESSAGE;
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return ValueCreator.createArrayValue(dynamicMessage.toByteArray());
    }

    private static Builder buildDynamicMessageFromType(Object anydata, Descriptor schema, Type type) {

        Builder messageBuilder = DynamicMessage.newBuilder(schema);

        switch (type.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                Descriptor messageDescriptor = messageBuilder.getDescriptorForType();
                FieldDescriptor field = messageDescriptor.findFieldByName(Constants.ATOMIC_FIELD_NAME);
                return generateMessageForPrimitiveType(messageBuilder, field, anydata, type.getName());
            }

            case TypeTags.DECIMAL_TAG: {
                Descriptor messageDescriptor = messageBuilder.getDescriptorForType();
                return generateMessageForPrimitiveDecimalType(messageBuilder, anydata, messageDescriptor);
            }

            case TypeTags.UNION_TAG: {
                return generateMessageForUnionType(messageBuilder, anydata);
            }

            case TypeTags.ARRAY_TAG: {
                int dimensions = Utils.getDimensions((ArrayType) type);
                Descriptor messageDescriptor = messageBuilder.getDescriptorForType();
                String messageName = Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + dimensions;
                FieldDescriptor field = messageDescriptor.findFieldByName(messageName);
                return generateMessageForArrayType(messageBuilder, field, (BArray) anydata, dimensions);
            }

            case TypeTags.RECORD_TYPE_TAG: {
                @SuppressWarnings("unchecked") BMap<BString, Object> record = (BMap<BString, Object>) anydata;
                return generateMessageForRecordType(messageBuilder, record);
            }

            default:
                throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    private static Builder generateMessageForPrimitiveType(
            Builder messageBuilder, FieldDescriptor field, Object anydata, String ballerinaType) {

        if (ballerinaType.equals(Constants.BYTE)) {
            byte[] data = new byte[]{((Integer) anydata).byteValue()};
            messageBuilder.setField(field, data);
        } else if (ballerinaType.equals(Constants.STRING)) {
            BString bString = (BString) anydata;
            messageBuilder.setField(field, bString.getValue());
        } else {
            messageBuilder.setField(field, anydata);
        }

        return messageBuilder;
    }

    private static Builder generateMessageForPrimitiveDecimalType(
            Builder messageBuilder, Object anydata, Descriptor decimalSchema) {

        BigDecimal bigDecimal = ((BDecimal) anydata).decimalValue();

        FieldDescriptor scale = decimalSchema.findFieldByName(Constants.SCALE);
        FieldDescriptor precision = decimalSchema.findFieldByName(Constants.PRECISION);
        FieldDescriptor value = decimalSchema.findFieldByName(Constants.VALUE);

        messageBuilder.setField(scale, bigDecimal.scale());
        messageBuilder.setField(precision, bigDecimal.precision());
        messageBuilder.setField(value, bigDecimal.unscaledValue().toByteArray());

        return messageBuilder;
    }


    private static Builder generateMessageForUnionType(Builder messageBuilder, Object anydata) {

        Descriptor messageDescriptor = messageBuilder.getDescriptorForType();

        if (anydata == null) {
            FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(Constants.NULL_FIELD_NAME);
            messageBuilder.setField(fieldDescriptor, true);
            return messageBuilder;
        }

        String javaType = anydata.getClass().getSimpleName();
        String ballerinaType = DataTypeMapper.mapJavaTypeToBallerinaType(javaType);

        // Handle all ballerina primitive values
        if (DataTypeMapper.isValidJavaType(javaType)) {
            String fieldName = ballerinaType + Constants.TYPE_SEPARATOR + Constants.UNION_FIELD_NAME;
            FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);

            // Handle decimal type
            if (ballerinaType.equals(Constants.DECIMAL)) {
                Descriptor decimalSchema = fieldDescriptor.getMessageType();
                Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(
                        decimalMessageBuilder, anydata, decimalSchema)
                        .build();
                messageBuilder.setField(fieldDescriptor, decimalMessage);
                return messageBuilder;
            }

            // Handle byte type
            if (fieldDescriptor == null && javaType.equals(Constants.INTEGER)) {
                fieldName = Constants.BYTE + Constants.TYPE_SEPARATOR + Constants.UNION_FIELD_NAME;
                fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                assert fieldDescriptor != null;
            }

            return generateMessageForPrimitiveType(messageBuilder, fieldDescriptor, anydata, ballerinaType);
        }

        if (anydata instanceof BArray) {
            BArray bArray = (BArray) anydata;
            ballerinaType = bArray.getElementType().getName();
            int dimention = 1;

            if (ballerinaType.equals(Constants.EMPTY_STRING)) {
                // Get the base type of the ballerina multidimensional array
                ballerinaType = Utils.getElementTypeOfBallerinaArray((ArrayType) bArray.getElementType());
                dimention += Utils.getDimensions((ArrayType) bArray.getElementType());
            }

            if (!DataTypeMapper.isBallerinaPrimitiveType(ballerinaType)) {
                ballerinaType = Constants.UNION + Constants.SEPARATOR + ballerinaType;
            }


            String fieldName = ballerinaType + Constants.TYPE_SEPARATOR
                    + Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + dimention
                    + Constants.TYPE_SEPARATOR + Constants.UNION_FIELD_NAME;

            FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
            return generateMessageForArrayType(messageBuilder, fieldDescriptor, bArray, dimention, ballerinaType);
        }

        return messageBuilder;
    }

    private static Builder generateMessageForArrayType(
            Builder messageBuilder, FieldDescriptor field, BArray bArray, int dimensions) {

        return generateMessageForArrayType(messageBuilder, field, bArray, dimensions, null);
    }

    private static Builder generateMessageForArrayType(
            Builder messageBuilder, FieldDescriptor field, BArray bArray, int dimensions, String ballerinaTypePrefix) {

        int len = bArray.size();
        Type type = bArray.getElementType();
        Descriptor schema = messageBuilder.getDescriptorForType();

        for (int i = 0; i < len; i++) {

            Object element = bArray.get(i);

            switch (type.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    messageBuilder.addRepeatedField(field, element);
                    break;
                }

                case TypeTags.BYTE_TAG: {
                    // Protobuf support bytes, set byte[] in the field rather than looping over elements
                    messageBuilder.setField(field, bArray.getBytes());
                    return messageBuilder;
                }

                case TypeTags.STRING_TAG: {
                    BString bString = (BString) element;
                    messageBuilder.addRepeatedField(field, bString.getValue());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = field.getMessageType();
                    Builder decimalBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(
                            decimalBuilder, element, decimalSchema).build();
                    messageBuilder.addRepeatedField(field, decimalMessage);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    String nestedTypeName = field.toProto().getTypeName();
                    Descriptor nestedSchema = schema.findNestedTypeByName(nestedTypeName);
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    DynamicMessage nestedMessage = generateMessageForUnionType(nestedMessageBuilder, element)
                            .build();
                    messageBuilder.addRepeatedField(field, nestedMessage);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    String nestedTypeName = Constants.ARRAY_BUILDER_NAME + Constants.SEPARATOR + (dimensions - 1);
                    String nestedFieldName = Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + (dimensions - 1);

                    if (ballerinaTypePrefix != null) {
                        nestedTypeName = ballerinaTypePrefix + Constants.TYPE_SEPARATOR + nestedTypeName;
                    }

                    Descriptor nestedSchema = schema.findNestedTypeByName(nestedTypeName);
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    Descriptor messageDescriptor = nestedMessageBuilder.getDescriptorForType();
                    FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(nestedFieldName);

                    DynamicMessage nestedMessage = generateMessageForArrayType(
                            nestedMessageBuilder, fieldDescriptor, (BArray) element, dimensions - 1)
                            .build();
                    messageBuilder.addRepeatedField(field, nestedMessage);
                    break;
                }
            }
        }
        return messageBuilder;
    }

    private static Builder generateMessageForRecordType(
            Builder messageBuilder, BMap<BString, Object> record) {

        RecordType recordType = (RecordType) record.getType();
        Descriptor schema = messageBuilder.getDescriptorForType();

        for (var entry : recordType.getFields().entrySet()) {
            String entryFieldName = entry.getKey();
            Type entryFieldType = entry.getValue().getFieldType();

            Object entryValue = record.get(StringUtils.fromString(entryFieldName));
            FieldDescriptor field = schema.findFieldByName(entryFieldName);

            switch (entryFieldType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    generateMessageForPrimitiveType(messageBuilder, field, entryValue, entryFieldType.getName());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = field.getMessageType();
                    Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(
                            decimalMessageBuilder, entryValue, decimalSchema)
                            .build();
                    messageBuilder.setField(field, decimalMessage);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    Descriptor nestedSchema = field.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    DynamicMessage nestedMessage = generateMessageForUnionType(nestedMessageBuilder, entryValue)
                            .build();
                    messageBuilder.setField(field, nestedMessage);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    int dimensions = Utils.getDimensions((ArrayType) entryFieldType);
                    generateMessageForArrayType(messageBuilder, field, (BArray) entryValue, dimensions);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    @SuppressWarnings("unchecked")
                    BMap<BString, Object> nestedRecord = (BMap<BString, Object>) entryValue;
                    Builder recordBuilder = DynamicMessage.newBuilder(field.getMessageType());
                    DynamicMessage nestedMessage = generateMessageForRecordType(recordBuilder,  nestedRecord).build();
                    messageBuilder.setField(field, nestedMessage);
                    break;
                }
            }
        }
        return messageBuilder;
    }
}
