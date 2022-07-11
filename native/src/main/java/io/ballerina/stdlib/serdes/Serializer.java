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
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME;
import static io.ballerina.stdlib.serdes.Constants.BYTE;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.SERIALIZATION_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.TYPE_MISMATCH_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Serializer class to create a byte array for a value.
 */
public class Serializer {

    /**
     * Creates a BArray for given data after serializing.
     *
     * @param ser     Serializer object.
     * @param anydata Data that is being serialized.
     * @return Byte array of the serialized value.
     */
    @SuppressWarnings("unused")
    public static Object serialize(BObject ser, Object anydata) {
        BTypedesc bTypedesc = (BTypedesc) ser.get(BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
        Descriptor messageDescriptor = (Descriptor) ser.getNativeData(SCHEMA_NAME);
        DynamicMessage dynamicMessage;
        try {
            dynamicMessage = buildDynamicMessageFromType(anydata, messageDescriptor,
                    bTypedesc.getDescribingType()).build();
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (IllegalArgumentException e) {
            String errorMessage = SERIALIZATION_ERROR_MESSAGE + TYPE_MISMATCH_ERROR_MESSAGE;
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return ValueCreator.createArrayValue(dynamicMessage.toByteArray());
    }

    private static Builder buildDynamicMessageFromType(Object anydata, Descriptor messageDescriptor,
                                                       Type ballerinaType) {
        Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor);
        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(ATOMIC_FIELD_NAME);
                return generateMessageForPrimitiveType(messageBuilder, fieldDescriptor, anydata,
                        referredType.getName());
            }

            case TypeTags.DECIMAL_TAG: {
                return generateMessageForPrimitiveDecimalType(messageBuilder, anydata, messageDescriptor);
            }

            case TypeTags.UNION_TAG: {
                return generateMessageForUnionType(messageBuilder, anydata);
            }

            case TypeTags.ARRAY_TAG: {
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(ARRAY_FIELD_NAME);
                return generateMessageForArrayType(messageBuilder, fieldDescriptor, (BArray) anydata);
            }

            case TypeTags.RECORD_TYPE_TAG: {
                @SuppressWarnings("unchecked") BMap<BString, Object> record = (BMap<BString, Object>) anydata;
                return generateMessageForRecordType(messageBuilder, record);
            }

            case TypeTags.MAP_TAG: {
                @SuppressWarnings("unchecked") BMap<BString, Object> ballerinaMap = (BMap<BString, Object>) anydata;
                return generateMessageForMapType(messageBuilder, ballerinaMap);
            }

            case TypeTags.TABLE_TAG: {
                BTable<?, ?> table = (BTable<?, ?>) anydata;
                return generateMessageForTableType(messageBuilder, table);
            }

            case TypeTags.TUPLE_TAG: {
                BArray tuple = (BArray) anydata;
                return generateMessageForTupleType(messageBuilder, tuple);
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }

    private static Builder generateMessageForPrimitiveType(Builder messageBuilder, FieldDescriptor field,
                                                           Object anydata, String ballerinaTypeName) {

        if (ballerinaTypeName.equals(BYTE)) {
            byte[] data = new byte[]{((Integer) anydata).byteValue()};
            messageBuilder.setField(field, data);
        } else if (ballerinaTypeName.equals(STRING)) {
            BString bString = (BString) anydata;
            messageBuilder.setField(field, bString.getValue());
        } else {
            messageBuilder.setField(field, anydata);
        }

        return messageBuilder;
    }

    private static Builder generateMessageForPrimitiveDecimalType(Builder messageBuilder, Object anydata,
                                                                  Descriptor decimalSchema) {
        BigDecimal bigDecimal = ((BDecimal) anydata).decimalValue();

        FieldDescriptor scale = decimalSchema.findFieldByName(SCALE);
        FieldDescriptor precision = decimalSchema.findFieldByName(PRECISION);
        FieldDescriptor value = decimalSchema.findFieldByName(VALUE);

        messageBuilder.setField(scale, bigDecimal.scale());
        messageBuilder.setField(precision, bigDecimal.precision());
        messageBuilder.setField(value, bigDecimal.unscaledValue().toByteArray());

        return messageBuilder;
    }


    private static Builder generateMessageForUnionType(Builder messageBuilder, Object anydata) {
        Descriptor messageDescriptor = messageBuilder.getDescriptorForType();
        Type type = TypeUtils.getType(anydata);
        Type referredType = TypeUtils.getReferredType(type);

        switch (type.getTag()) {
            case TypeTags.NULL_TAG: {
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(NULL_FIELD_NAME);
                messageBuilder.setField(fieldDescriptor, true);
                return messageBuilder;
            }

            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String fieldName = referredType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                return generateMessageForPrimitiveType(messageBuilder, fieldDescriptor, anydata,
                        referredType.getName());
            }

            case TypeTags.STRING_TAG: {
                String fieldName =  referredType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);

                if (fieldDescriptor == null) {
                    // enum value
                    fieldName = ((BString) anydata).getValue();
                    fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                }
                return generateMessageForPrimitiveType(messageBuilder, fieldDescriptor, anydata,
                        referredType.getName());
            }

            case TypeTags.DECIMAL_TAG: {
                String fieldName = referredType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                Descriptor decimalSchema = fieldDescriptor.getMessageType();
                Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(decimalMessageBuilder, anydata,
                        decimalSchema).build();
                messageBuilder.setField(fieldDescriptor, decimalMessage);
                return messageBuilder;
            }

            case TypeTags.ARRAY_TAG: {
                BArray bArray = (BArray) anydata;
                String ballerinaType = bArray.getElementType().getName();
                int dimention = 1;

                if (ballerinaType.equals(EMPTY_STRING)) {
                    // Get the base type of the ballerina multidimensional array
                    ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) bArray.getElementType());
                    dimention += Utils.getArrayDimensions((ArrayType) bArray.getElementType());
                }

                String fieldName = ballerinaType + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention
                        + TYPE_SEPARATOR + UNION_FIELD_NAME;

                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                return generateMessageForArrayType(messageBuilder, fieldDescriptor, bArray);
            }

            case TypeTags.RECORD_TYPE_TAG: {
                @SuppressWarnings("unchecked") BMap<BString, Object> ballerinaMapOrRecord
                        = (BMap<BString, Object>) anydata;
                String recordTypename = ballerinaMapOrRecord.getType().getName();
                String fieldName = recordTypename + TYPE_SEPARATOR + UNION_FIELD_NAME;
                FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                Descriptor recordSchema = fieldDescriptor.getMessageType();
                Builder recordMessageBuilder = DynamicMessage.newBuilder(recordSchema);
                DynamicMessage recordMessage = generateMessageForRecordType(recordMessageBuilder,
                        ballerinaMapOrRecord).build();
                messageBuilder.setField(fieldDescriptor, recordMessage);
                return messageBuilder;
            }

            case TypeTags.TUPLE_TAG: {
                BArray bArray = (BArray) anydata;
                String fieldName = bArray.getType().getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                FieldDescriptor tupleFieldDescriptor = messageDescriptor.findFieldByName(fieldName);
                Descriptor tupleSchema = tupleFieldDescriptor.getMessageType();
                Builder tupleMessageBuilder = DynamicMessage.newBuilder(tupleSchema);
                generateMessageForTupleType(tupleMessageBuilder, bArray);
                messageBuilder.setField(tupleFieldDescriptor, tupleMessageBuilder.build());
                return messageBuilder;
            }


            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }

    private static Builder generateMessageForArrayType(Builder messageBuilder, FieldDescriptor fieldDescriptor,
                                                       BArray bArray) {
        int len = bArray.size();
        Type elementType = bArray.getElementType();
        Type referredElementType = TypeUtils.getReferredType(elementType);

        for (int i = 0; i < len; i++) {

            Object element = bArray.get(i);

            switch (referredElementType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    messageBuilder.addRepeatedField(fieldDescriptor, element);
                    break;
                }

                case TypeTags.BYTE_TAG: {
                    // Protobuf support bytes, set byte[] in the field rather than looping over elements
                    messageBuilder.setField(fieldDescriptor, bArray.getBytes());
                    return messageBuilder;
                }

                case TypeTags.STRING_TAG: {
                    BString bString = (BString) element;
                    messageBuilder.addRepeatedField(fieldDescriptor, bString.getValue());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = fieldDescriptor.getMessageType();
                    Builder decimalBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(decimalBuilder, element,
                            decimalSchema).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, decimalMessage);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    DynamicMessage nestedMessage = generateMessageForUnionType(nestedMessageBuilder, element).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    Descriptor nestedMessageDescriptor = nestedMessageBuilder.getDescriptorForType();

                    FieldDescriptor nestedFieldDescriptor = nestedMessageDescriptor.findFieldByName(ARRAY_FIELD_NAME);

                    DynamicMessage nestedMessage = generateMessageForArrayType(nestedMessageBuilder,
                            nestedFieldDescriptor, (BArray) element).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    @SuppressWarnings("unchecked") BMap<BString, Object> recordMap = (BMap<BString, Object>) element;
                    DynamicMessage recordMessage = generateMessageForRecordType(nestedMessageBuilder,
                            recordMap).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, recordMessage);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    @SuppressWarnings("unchecked") BMap<BString, Object> ballerinaMap = (BMap<BString, Object>) element;
                    DynamicMessage mapMessage = generateMessageForMapType(nestedMessageBuilder, ballerinaMap).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, mapMessage);
                    break;
                }

                case TypeTags.TABLE_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    BTable<?, ?> table = (BTable<?, ?>) element;
                    DynamicMessage tableMessage = generateMessageForTableType(nestedMessageBuilder, table).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, tableMessage);
                    break;
                }

                case TypeTags.TUPLE_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    BArray tuple = (BArray) element;
                    DynamicMessage tupleMessage = generateMessageForTupleType(nestedMessageBuilder, tuple).build();
                    messageBuilder.addRepeatedField(fieldDescriptor, tupleMessage);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredElementType.getName(), SERDES_ERROR);
            }
        }
        return messageBuilder;
    }

    private static Builder generateMessageForRecordType(Builder messageBuilder, BMap<BString, Object> record) {
        Descriptor schema = messageBuilder.getDescriptorForType();
        RecordType recordType = (RecordType) record.getType();
        Map<String, Field> recordTypeFields = recordType.getFields();

        for (Map.Entry<BString, Object> recordField : record.entrySet()) {
            String recordFieldName = recordField.getKey().getValue();
            Object recordFieldValue = recordField.getValue();

            Type recordFieldType = recordTypeFields.get(recordFieldName).getFieldType();
            Type referredRecordFieldType = TypeUtils.getReferredType(recordFieldType);
            FieldDescriptor fieldDescriptor = schema.findFieldByName(recordFieldName);

            switch (referredRecordFieldType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    generateMessageForPrimitiveType(messageBuilder, fieldDescriptor, recordFieldValue,
                            referredRecordFieldType.getName());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = fieldDescriptor.getMessageType();
                    Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(decimalMessageBuilder,
                            recordFieldValue, decimalSchema).build();
                    messageBuilder.setField(fieldDescriptor, decimalMessage);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    Descriptor nestedSchema = fieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
                    DynamicMessage nestedMessage = generateMessageForUnionType(nestedMessageBuilder,
                            recordFieldValue).build();
                    messageBuilder.setField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    generateMessageForArrayType(messageBuilder, fieldDescriptor, (BArray) recordFieldValue);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> nestedRecord
                            = (BMap<BString, Object>) recordFieldValue;
                    Builder mapBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
                    DynamicMessage nestedMessage = generateMessageForRecordType(mapBuilder, nestedRecord).build();
                    messageBuilder.setField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> map = (BMap<BString, Object>) recordFieldValue;
                    Builder mapBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
                    DynamicMessage nestedMessage = generateMessageForMapType(mapBuilder, map).build();
                    messageBuilder.setField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.TABLE_TAG: {
                    BTable<?, ?> table = (BTable<?, ?>) recordFieldValue;
                    Builder tableBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
                    DynamicMessage nestedMessage = generateMessageForTableType(tableBuilder, table).build();
                    messageBuilder.setField(fieldDescriptor, nestedMessage);
                    break;
                }

                case TypeTags.TUPLE_TAG: {
                    BArray tuple = (BArray) recordFieldValue;
                    Builder tableBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
                    DynamicMessage nestedMessage = generateMessageForTupleType(tableBuilder, tuple).build();
                    messageBuilder.setField(fieldDescriptor, nestedMessage);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredRecordFieldType.getName(), SERDES_ERROR);
            }
        }
        return messageBuilder;
    }


    private static Builder generateMessageForMapType(Builder messageBuilder, BMap<BString, Object> ballerinaMap) {

        Descriptor schema = messageBuilder.getDescriptorForType();
        MapType mapType = (MapType) ballerinaMap.getType();
        Type constrainedType = mapType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);

        FieldDescriptor mapFieldDescriptor = schema.findFieldByName(MAP_FIELD);
        Descriptor mapEntryDescriptor = mapFieldDescriptor.getMessageType();
        FieldDescriptor keyFieldDescriptor = mapEntryDescriptor.findFieldByName(KEY_NAME);
        FieldDescriptor valueFieldDescriptor = mapEntryDescriptor.findFieldByName(VALUE_NAME);

        Builder mapFieldMessage = DynamicMessage.newBuilder(mapEntryDescriptor);

        for (Map.Entry<BString, Object> mapEntry : ballerinaMap.entrySet()) {
            String keyName = mapEntry.getKey().getValue();
            Object value = mapEntry.getValue();

            mapFieldMessage.setField(keyFieldDescriptor, keyName);

            switch (referredConstrainedType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.BOOLEAN_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.STRING_TAG: {
                    generateMessageForPrimitiveType(mapFieldMessage, valueFieldDescriptor, value,
                            referredConstrainedType.getName());
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = valueFieldDescriptor.getMessageType();
                    Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(decimalMessageBuilder, value,
                            decimalSchema).build();
                    mapFieldMessage.setField(valueFieldDescriptor, decimalMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.UNION_TAG: {
                    Descriptor nestedUnionSchema = valueFieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(nestedUnionSchema);
                    DynamicMessage nestedUnionMessage = generateMessageForUnionType(nestedMessageBuilder,
                            value).build();
                    mapFieldMessage.setField(valueFieldDescriptor, nestedUnionMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    generateMessageForArrayType(mapFieldMessage, valueFieldDescriptor, (BArray) value);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> nestedRecord = (BMap<BString, Object>) value;
                    Builder recordBuilder = DynamicMessage.newBuilder(valueFieldDescriptor.getMessageType());
                    DynamicMessage nestedRecordMessage = generateMessageForRecordType(recordBuilder,
                            nestedRecord).build();
                    mapFieldMessage.setField(valueFieldDescriptor, nestedRecordMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.MAP_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> nestedMap = (BMap<BString, Object>) value;
                    Builder mapBuilder = DynamicMessage.newBuilder(valueFieldDescriptor.getMessageType());
                    DynamicMessage nestedMapMessage = generateMessageForMapType(mapBuilder, nestedMap).build();
                    mapFieldMessage.setField(valueFieldDescriptor, nestedMapMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.TABLE_TAG: {
                    BTable<?, ?> table = (BTable<?, ?>) value;
                    Builder tableBuilder = DynamicMessage.newBuilder(valueFieldDescriptor.getMessageType());
                    DynamicMessage nestedMapMessage = generateMessageForTableType(tableBuilder, table).build();
                    mapFieldMessage.setField(valueFieldDescriptor, nestedMapMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                case TypeTags.TUPLE_TAG: {
                    BArray tuple = (BArray) value;
                    Builder tupleBuilder = DynamicMessage.newBuilder(valueFieldDescriptor.getMessageType());
                    DynamicMessage nestedMapMessage = generateMessageForTupleType(tupleBuilder, tuple).build();
                    mapFieldMessage.setField(valueFieldDescriptor, nestedMapMessage);
                    messageBuilder.addRepeatedField(mapFieldDescriptor, mapFieldMessage.build());
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredConstrainedType.getName(), SERDES_ERROR);
            }
        }
        return messageBuilder;
    }

    private static Builder generateMessageForTableType(Builder messageBuilder, BTable<?, ?> table) {
        Type constrainedType = ((TableType) TypeUtils.getType(table)).getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        FieldDescriptor tableEntryField = messageBuilder.getDescriptorForType().findFieldByName(TABLE_ENTRY);
        Descriptor tableEntrySchema = tableEntryField.getMessageType();

        for (Object value : table.values()) {
            @SuppressWarnings("unchecked") BMap<BString, Object> recordOrMap = (BMap<BString, Object>) value;
            // Create a nested message for each entry
            Builder nestedMessageBuilder = DynamicMessage.newBuilder(tableEntrySchema);

            switch (constrainedType.getTag()) {
                case TypeTags.RECORD_TYPE_TAG: {
                    generateMessageForRecordType(nestedMessageBuilder, recordOrMap);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    generateMessageForMapType(nestedMessageBuilder, recordOrMap);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredConstrainedType.getName(), SERDES_ERROR);

            }
            messageBuilder.addRepeatedField(tableEntryField, nestedMessageBuilder.build());
        }
        return messageBuilder;
    }


    private static Builder generateMessageForTupleType(Builder messageBuilder, BArray tuple) {
        int elementFieldNumber = 1;

        for (Object value : tuple.getValues()) {
            String elementFieldName = TUPLE_FIELD_NAME + SEPARATOR + elementFieldNumber;
            FieldDescriptor tupleElementFieldDescriptor = messageBuilder.getDescriptorForType()
                    .findFieldByName(elementFieldName);
            Type elementType = ((TupleType) tuple.getType()).getTupleTypes().get(elementFieldNumber - 1);
            Type referredElementType = TypeUtils.getReferredType(elementType);

            switch (referredElementType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    generateMessageForPrimitiveType(messageBuilder, tupleElementFieldDescriptor, value,
                            referredElementType.getName());
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    Descriptor decimalSchema = tupleElementFieldDescriptor.getMessageType();
                    Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalSchema);
                    DynamicMessage decimalMessage = generateMessageForPrimitiveDecimalType(decimalMessageBuilder, value,
                            decimalSchema).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, decimalMessage);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    Descriptor unionSchema = tupleElementFieldDescriptor.getMessageType();
                    Builder nestedMessageBuilder = DynamicMessage.newBuilder(unionSchema);
                    DynamicMessage nestedUnionMessage = generateMessageForUnionType(nestedMessageBuilder,
                            value).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, nestedUnionMessage);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    generateMessageForArrayType(messageBuilder, tupleElementFieldDescriptor, (BArray) value);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> record = (BMap<BString, Object>) value;
                    Builder recordBuilder = DynamicMessage.newBuilder(tupleElementFieldDescriptor.getMessageType());
                    DynamicMessage nestedRecordMessage = generateMessageForRecordType(recordBuilder, record).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, nestedRecordMessage);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    @SuppressWarnings("unchecked") BMap<BString, Object> nestedMap = (BMap<BString, Object>) value;
                    Builder mapBuilder = DynamicMessage.newBuilder(tupleElementFieldDescriptor.getMessageType());
                    DynamicMessage nestedMapMessage = generateMessageForMapType(mapBuilder, nestedMap).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, nestedMapMessage);
                    break;
                }

                case TypeTags.TABLE_TAG: {
                    BTable<?, ?> table = (BTable<?, ?>) value;
                    Builder tableBuilder = DynamicMessage.newBuilder(tupleElementFieldDescriptor.getMessageType());
                    DynamicMessage nestedMapMessage = generateMessageForTableType(tableBuilder, table).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, nestedMapMessage);
                    break;
                }

                case TypeTags.TUPLE_TAG: {
                    BArray nestedTuple = (BArray) value;
                    Builder nestedTupleBuilder = DynamicMessage.newBuilder(
                            tupleElementFieldDescriptor.getMessageType());
                    DynamicMessage nestedTupleMessage = generateMessageForTupleType(nestedTupleBuilder,
                            nestedTuple).build();
                    messageBuilder.setField(tupleElementFieldDescriptor, nestedTupleMessage);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredElementType.getName(), SERDES_ERROR);

            }
            elementFieldNumber++;
        }
        return messageBuilder;
    }
}
