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
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufFileBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Builder;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
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
            serdes.addNativeData(Constants.SCHEMA_NAME, schema);
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (DescriptorValidationException e) {
            String errorMessage = Constants.SCHEMA_GENERATION_FAILURE + e.getMessage();
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return null;
    }

    private static ProtobufMessageBuilder buildProtobufMessageFromBallerinaTypedesc(Type ballerinaType) {

        ProtobufMessageBuilder messageBuilder;
        int fieldNumber = 1;

        switch (ballerinaType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                messageBuilder = new ProtobufMessageBuilder(Utils.createMessageName(ballerinaType.getName()));
                String atomicFieldName = Constants.ATOMIC_FIELD_NAME;
                generateMessageDefinitionForPrimitiveType(messageBuilder, ballerinaType, atomicFieldName, fieldNumber);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                messageBuilder = new ProtobufMessageBuilder(Utils.createMessageName(ballerinaType.getName()));
                generateMessageDefinitionForPrimitiveDecimal(messageBuilder);
                break;
            }

            case TypeTags.UNION_TAG: {
                messageBuilder = new ProtobufMessageBuilder(Constants.UNION_BUILDER_NAME);
                generateMessageDefinitionForUnionType(messageBuilder, (UnionType) ballerinaType);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType arrayType = (ArrayType) ballerinaType;
                int dimensions = Utils.getDimensions(arrayType);
                String messageName = Constants.ARRAY_BUILDER_NAME + Constants.SEPARATOR + dimensions;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForArrayType(messageBuilder, arrayType, dimensions, fieldNumber, false);
                break;
            }

            default:
                throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + ballerinaType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(
            ProtobufMessageBuilder messageBuilder, Type ballerinaType, String fieldName, int fieldNumber) {

        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        Builder messageField = ProtobufMessageFieldBuilder.newFieldBuilder(
                Constants.OPTIONAL_LABEL, protoType, fieldName, fieldNumber);

        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(
            ProtobufMessageBuilder messageBuilder) {

        int fieldNumber = 1;

        // Uses java BigDecimal properties; scale, precision and value as message fields
        Builder scaleField = ProtobufMessageFieldBuilder
                .newFieldBuilder(Constants.OPTIONAL_LABEL, Constants.UINT32, Constants.SCALE, fieldNumber++);
        Builder precisionField = ProtobufMessageFieldBuilder
                .newFieldBuilder(Constants.OPTIONAL_LABEL, Constants.UINT32, Constants.PRECISION, fieldNumber++);
        Builder valueField = ProtobufMessageFieldBuilder
                .newFieldBuilder(Constants.OPTIONAL_LABEL, Constants.BYTES, Constants.VALUE, fieldNumber);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }

    private static void generateMessageDefinitionForUnionType(
            ProtobufMessageBuilder messageBuilder, UnionType unionType) {

        int fieldNumber = 1;

        // Member field names are prefixed with ballerina type name to avoid name collision in proto message definition
        for (Type memberType : unionType.getMemberTypes()) {
            switch (memberType.getTag()) {
                case TypeTags.NULL_TAG: {
                    String fieldName = Constants.NULL_FIELD_NAME;
                    Builder nilField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(Constants.OPTIONAL_LABEL, Constants.BOOL, fieldName, fieldNumber);
                    messageBuilder.addField(nilField);
                    break;
                }

                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    String fieldName = memberType.getName() + Constants.TYPE_SEPARATOR + Constants.UNION_FIELD_NAME;
                    generateMessageDefinitionForPrimitiveType(messageBuilder, memberType, fieldName, fieldNumber);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    String fieldName = memberType.getName() + Constants.TYPE_SEPARATOR + Constants.UNION_FIELD_NAME;

                    ProtobufMessageBuilder decimalMessageBuilder = new ProtobufMessageBuilder(Constants.DECIMAL_VALUE);
                    generateMessageDefinitionForPrimitiveDecimal(decimalMessageBuilder);
                    messageBuilder.addNestedMessage(decimalMessageBuilder);

                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(memberType.getTag());
                    Builder messageField = ProtobufMessageFieldBuilder
                            .newFieldBuilder(Constants.OPTIONAL_LABEL, protoType, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) memberType;
                    int dimention = Utils.getDimensions(arrayType);
                    generateMessageDefinitionForArrayType(messageBuilder, arrayType, dimention, fieldNumber, true);
                    break;
                }

                default:
                    throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + memberType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }
    }

    private static void generateMessageDefinitionForArrayType(
            ProtobufMessageBuilder messageBuilder, ArrayType arrayType, int dimensions, int fieldNumber,
            boolean isUnionField) {

        Type type = arrayType.getElementType();
        String fieldName = Constants.ARRAY_FIELD_NAME + Constants.SEPARATOR + dimensions;

        switch (type.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(type.getTag());
                String label = protoType.equals(Constants.BYTES) ? Constants.OPTIONAL_LABEL : Constants.REPEATED_LABEL;

                if (isUnionField) {
                    fieldName = type.getName() + Constants.TYPE_SEPARATOR
                            + fieldName + Constants.TYPE_SEPARATOR
                            + Constants.UNION_FIELD_NAME;
                }

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(label, protoType, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }


            case TypeTags.DECIMAL_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(type.getTag());
                String label = Constants.REPEATED_LABEL;

                if (isUnionField) {
                    fieldName = type.getName() + Constants.TYPE_SEPARATOR
                            + fieldName + Constants.TYPE_SEPARATOR
                            + Constants.UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder decimalMessageBuilder = new ProtobufMessageBuilder(protoType);
                generateMessageDefinitionForPrimitiveDecimal(decimalMessageBuilder);
                messageBuilder.addNestedMessage(decimalMessageBuilder);
                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(label, protoType, fieldName, fieldNumber);

                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.UNION_TAG: {
                String nestedMessageName = Constants.UNION_BUILDER_NAME;

                if (isUnionField) {
                    String ballerinaUnionTypeName = Utils.getElementTypeOfBallerinaArray(arrayType);
                    // ballerinaType becomes "union_<BallerinaUnionTypeName>"
                    String ballerinaType = Constants.UNION + Constants.SEPARATOR + ballerinaUnionTypeName;
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    nestedMessageName = ballerinaType + Constants.TYPE_SEPARATOR + nestedMessageName;
                    // fieldName becomes "union_<BallerinaUnionTypeName>__arrayFieldName_<dimention>__unionField"
                    fieldName = ballerinaType + Constants.TYPE_SEPARATOR
                            + fieldName + Constants.TYPE_SEPARATOR
                            + Constants.UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) type);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(Constants.REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType nestedArrayType = (ArrayType) type;
                String nestedMessageName = Constants.ARRAY_BUILDER_NAME + Constants.SEPARATOR + (dimensions - 1);

                if (isUnionField) {
                    String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
                    if (!DataTypeMapper.isBallerinaPrimitiveType(ballerinaType)) {
                        ballerinaType = Constants.UNION + Constants.SEPARATOR + ballerinaType;
                    }
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    nestedMessageName = ballerinaType + Constants.TYPE_SEPARATOR + nestedMessageName;
                    fieldName = ballerinaType + Constants.TYPE_SEPARATOR
                            + fieldName + Constants.TYPE_SEPARATOR
                            + Constants.UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForArrayType(nestedMessageBuilder, nestedArrayType, dimensions - 1, 1, false);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                Builder messageField = ProtobufMessageFieldBuilder
                        .newFieldBuilder(Constants.REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            default:
                throw createSerdesError(Constants.UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }
}
