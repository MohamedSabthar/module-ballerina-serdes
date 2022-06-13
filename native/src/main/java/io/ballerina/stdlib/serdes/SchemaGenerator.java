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


import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufFileBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Builder;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.FAILED_WRITE_FILE;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.PROTO3;
import static io.ballerina.stdlib.serdes.Constants.RECORD;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_GENERATION_FAILURE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Generates a Protobuf schema for a given data type.
 */
public class SchemaGenerator {

    /**
     * Creates a schema for a given data type and adds to native data.
     *
     * @param serdes   Serializer or Deserializer object.
     * @param typedesc Data type that is being serialized.
     * @return {@code BError}, if there are schema generation errors, null otherwise.
     */
    @SuppressWarnings("unused")
    public static Object generateSchema(BObject serdes, BTypedesc typedesc) {
        try {
            ProtobufFileBuilder protobufFile = new ProtobufFileBuilder();
            ProtobufMessageBuilder protobufMessageBuilder = buildProtobufMessageFromBallerinaTypedesc(
                    typedesc.getDescribingType());
            Descriptor schema = protobufFile.addMessageType(protobufMessageBuilder).build();
            serdes.addNativeData(SCHEMA_NAME, schema);
            serdes.addNativeData(PROTO3, protobufFile.toString());
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (DescriptorValidationException e) {
            String errorMessage = SCHEMA_GENERATION_FAILURE + e.getMessage();
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return null;
    }

    @SuppressWarnings("unused")
    public static Object generateProtoFile(BObject serdes, BString filePath) {
        String filePathName = filePath.getValue();
        try (FileWriter file = new FileWriter(filePathName, StandardCharsets.UTF_8)) {
            String proto3 = (String) serdes.getNativeData(PROTO3);
            file.write(proto3);
        } catch (IOException e) {
            String errorMessage = FAILED_WRITE_FILE + e.getMessage();
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return null;
    }

    private static ProtobufMessageBuilder buildProtobufMessageFromBallerinaTypedesc(Type ballerinaType) {

        ProtobufMessageBuilder messageBuilder;
        String messageName;
        int fieldNumber = 1;

        switch (ballerinaType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                messageName = Utils.createMessageName(ballerinaType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveType(
                        messageBuilder,
                        ballerinaType,
                        ATOMIC_FIELD_NAME,
                        fieldNumber);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                messageName = Utils.createMessageName(ballerinaType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveDecimal(messageBuilder);
                break;
            }

            case TypeTags.UNION_TAG: {
                messageName = UNION_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForUnionType(messageBuilder, (UnionType) ballerinaType);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType arrayType = (ArrayType) ballerinaType;
                int dimensions = Utils.getDimensions(arrayType);
                messageName = ARRAY_BUILDER_NAME + SEPARATOR + dimensions;
                messageBuilder = new ProtobufMessageBuilder(messageName);

                String fieldName = ARRAY_FIELD_NAME + SEPARATOR + dimensions;
                generateMessageDefinitionForArrayType(
                        messageBuilder,
                        arrayType,
                        fieldName,
                        dimensions,
                        fieldNumber,
                        false);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) ballerinaType;
                messageName = recordType.getName();
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForRecordType(messageBuilder, recordType);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + ballerinaType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(
            ProtobufMessageBuilder messageBuilder,
            Type ballerinaType,
            String fieldName,
            int fieldNumber) {

        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        Builder messageField = ProtobufMessageFieldBuilder
                .newFieldBuilder(OPTIONAL_LABEL, protoType, fieldName, fieldNumber);
        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(ProtobufMessageBuilder messageBuilder) {

        int fieldNumber = 1;

        // Java BigDecimal representation used for serializing ballerina decimal value
        Builder scaleField = ProtobufMessageFieldBuilder.newFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, fieldNumber++);
        Builder precisionField = ProtobufMessageFieldBuilder
                .newFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION, fieldNumber++);
        Builder valueField = ProtobufMessageFieldBuilder.newFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, fieldNumber);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }

    private static void generateMessageDefinitionForUnionType(
            ProtobufMessageBuilder messageBuilder,
            UnionType unionType) {

        int fieldNumber = 1;
        String fieldName;

        // Member field names are prefixed with ballerina type name to avoid name collision in proto message definition
        for (Type memberType : unionType.getMemberTypes()) {
            switch (memberType.getTag()) {
                case TypeTags.NULL_TAG: {
                    fieldName = NULL_FIELD_NAME;
                    Builder nilField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, BOOL, fieldName, fieldNumber);
                    messageBuilder.addField(nilField);
                    break;
                }

                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    fieldName = memberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    generateMessageDefinitionForPrimitiveType(messageBuilder, memberType, fieldName, fieldNumber);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(DECIMAL_VALUE);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    fieldName = memberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(memberType.getTag());
                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, protoType, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                // Union of unions, no need to handle already it becomes a single flattened union

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) memberType;
                    int dimention = Utils.getDimensions(arrayType);
                    fieldName = ARRAY_FIELD_NAME + SEPARATOR + dimention;
                    generateMessageDefinitionForArrayType(
                            messageBuilder,
                            arrayType,
                            fieldName,
                            dimention,
                            fieldNumber,
                            true);
                    break;
                }


                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType recordType = (RecordType) memberType;
                    String nestedMessageName = recordType.getName();
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    fieldName = RECORD + SEPARATOR + nestedMessageName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, nestedMessageName, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + memberType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }
    }

    private static void generateMessageDefinitionForArrayType(
            ProtobufMessageBuilder messageBuilder,
            ArrayType arrayType,
            String fieldName,
            int dimensions,
            int fieldNumber,
            boolean isUnionField) {

        Type elementType = arrayType.getElementType();

        switch (elementType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(elementType.getTag());
                String label = protoType.equals(BYTES) ? OPTIONAL_LABEL : REPEATED_LABEL;

                if (isUnionField) {
                    fieldName = elementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(label, protoType, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }


            case TypeTags.DECIMAL_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(elementType.getTag());

                if (isUnionField) {
                    fieldName = elementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(REPEATED_LABEL, protoType, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.UNION_TAG: {
                String nestedMessageName = UNION_BUILDER_NAME;

                if (isUnionField) {
                    String ballerinaUnionTypeName = Utils.getElementTypeOfBallerinaArray(arrayType);
                    // ballerinaType becomes "union_<BallerinaUnionTypeName>"
                    String ballerinaType = UNION + SEPARATOR + ballerinaUnionTypeName;
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                    // fieldName becomes "union_<BallerinaUnionTypeName>__arrayFieldName_<dimention>__unionField"
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) elementType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType nestedArrayType = (ArrayType) elementType;
                String nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimensions - 1);

                if (isUnionField) {
                    String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
                    if (!DataTypeMapper.isBallerinaPrimitiveType(ballerinaType)) {
                        ballerinaType = UNION + SEPARATOR + ballerinaType;
                    }
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                String nestedMessageFieldName = ARRAY_FIELD_NAME + SEPARATOR + (dimensions - 1);
                generateMessageDefinitionForArrayType(
                        nestedMessageBuilder,
                        nestedArrayType,
                        nestedMessageFieldName,
                        dimensions - 1,
                        1,
                        false);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) elementType;
                String nestedMessageName = recordType.getName();

                if (isUnionField) {
                    String ballerinaType = RECORD + SEPARATOR + recordType.getName();
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + elementType.getName(), SERDES_ERROR);
        }
    }

    private static void generateMessageDefinitionForRecordType(
            ProtobufMessageBuilder messageBuilder,
            RecordType recordType) {

        Map<String, Field> recordFields = recordType.getFields();
        int fieldNumber = 1;

        for (var fieldEntry : recordFields.entrySet()) {
            String fieldEntryName = fieldEntry.getKey();
            Type fieldEntryType = fieldEntry.getValue().getFieldType();

            switch (fieldEntryType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(fieldEntryType.getTag());
                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(fieldEntryType.getTag());
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    String nestedMessageName = fieldEntryName + TYPE_SEPARATOR + UNION_BUILDER_NAME;
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) fieldEntryType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    Builder messageField = ProtobufMessageFieldBuilder.newFieldBuilder(
                            OPTIONAL_LABEL,
                            nestedMessageName,
                            fieldEntryName,
                            fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) fieldEntryType;
                    int dimention = Utils.getDimensions(arrayType);
                    generateMessageDefinitionForArrayType(
                            messageBuilder,
                            arrayType,
                            fieldEntryName,
                            dimention,
                            fieldNumber,
                            false);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType nestedRecordType = (RecordType) fieldEntryType;
                    String nestedMessageName = nestedRecordType.getName();
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, nestedRecordType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(OPTIONAL_LABEL, nestedMessageName, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                default:
                    throw createSerdesError(
                            UNSUPPORTED_DATA_TYPE + fieldEntryType.getName(),
                            SERDES_ERROR);
            }
            fieldNumber++;
        }
    }
}
