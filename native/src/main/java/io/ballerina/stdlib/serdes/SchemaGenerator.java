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
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
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

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.FAILED_WRITE_FILE;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.PROTO3;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_GENERATION_FAILURE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
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
     * @param serdes    Serializer or Deserializer object.
     * @param bTypedesc Data type that is being serialized.
     * @return {@code BError}, if there are schema generation errors, null otherwise.
     */
    @SuppressWarnings("unused")
    public static Object generateSchema(BObject serdes, BTypedesc bTypedesc) {
        try {
            ProtobufFileBuilder protobufFile = new ProtobufFileBuilder();
            ProtobufMessageBuilder protobufMessageBuilder = buildProtobufMessageFromBallerinaTypedesc(
                    bTypedesc.getDescribingType());
            Descriptor messageDescriptor = protobufFile.addMessageType(protobufMessageBuilder).build();
            serdes.addNativeData(SCHEMA_NAME, messageDescriptor);
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

        switch (ballerinaType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                int fieldNumber = 1;
                messageName = Utils.createMessageName(ballerinaType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveType(messageBuilder, ballerinaType, fieldNumber);
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
                BallerinaStructureTypeContext unionBallerinaStructureTypeContext = new BallerinaStructureTypeContext(
                        messageBuilder, ballerinaType);
                return unionBallerinaStructureTypeContext.schema();
            }

            case TypeTags.ARRAY_TAG: {
                messageName = ARRAY_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);

                BallerinaStructureTypeContext arrayBallerinaStructureTypeContext = new BallerinaStructureTypeContext(
                        messageBuilder, ballerinaType);
                return arrayBallerinaStructureTypeContext.schema();
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) ballerinaType;
                messageName = recordType.getName();
                messageBuilder = new ProtobufMessageBuilder(messageName);
                BallerinaStructureTypeContext recordBallerinaStructureTypeContext
                        = new BallerinaStructureTypeContext(messageBuilder, ballerinaType);
                return recordBallerinaStructureTypeContext.schema();
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + ballerinaType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(ProtobufMessageBuilder messageBuilder,
                                                                  Type ballerinaType, int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType,
                ATOMIC_FIELD_NAME, fieldNumber);
        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(ProtobufMessageBuilder messageBuilder) {
        int fieldNumber = 1;

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE,
                fieldNumber++);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                fieldNumber++);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE,
                fieldNumber);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }
}
