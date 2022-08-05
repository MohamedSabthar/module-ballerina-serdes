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
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Generates a Avro schema for a given data type.
 */
public class AvroSchemaGenerator {

    /**
     * Creates a schema for a given data type and adds to native data.
     *
     * @param serdes    Serializer or Deserializer object.
     * @param bTypedesc Data type that is being serialized.
     * @return {@code BError}, if there are schema generation errors, null otherwise.
     */
    @SuppressWarnings("unused")
    public static Object generateAvroSchema(BObject serdes, BTypedesc bTypedesc) {

        Schema schema = buildAvroSchemaFromBallerinaTypedesc(bTypedesc.getDescribingType());
        serdes.addNativeData(SCHEMA_NAME, schema);
        return null;
    }

    private static Schema buildAvroSchemaFromBallerinaTypedesc(Type ballerinaType) {
        Type referredType = TypeUtils.getReferredType(ballerinaType);
        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
                return SchemaBuilder.builder().longType();
            case TypeTags.BYTE_TAG:
                return SchemaBuilder.builder().bytesType();
            case TypeTags.FLOAT_TAG:
                return SchemaBuilder.builder().doubleType();
            case TypeTags.STRING_TAG:
                return SchemaBuilder.builder().stringType();
            case TypeTags.BOOLEAN_TAG:
                return SchemaBuilder.builder().booleanType();
            case TypeTags.DECIMAL_TAG:
                return LogicalTypes.decimal(34, 34).addToSchema(Schema.create(Schema.Type.BYTES));
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }
}
