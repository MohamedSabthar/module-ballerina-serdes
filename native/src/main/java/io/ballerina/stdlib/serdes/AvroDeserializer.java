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
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static io.ballerina.stdlib.serdes.Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME;
import static io.ballerina.stdlib.serdes.Constants.DESERIALIZATION_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Deserializer class to generate Ballerina value from byte array.
 */
public class AvroDeserializer {

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
            Schema schema = (Schema) des.getNativeData(SCHEMA_NAME);
            GenericDatumReader<?> datumReader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(encodedMessage.getBytes(), null);
            Object datum = datumReader.read(null, decoder);
            BTypedesc bTypedesc = (BTypedesc) des.get(BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
            return deserializeToBallerinaType(datum, bTypedesc.getDescribingType(), schema);
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (Exception e) {
            return createSerdesError(DESERIALIZATION_ERROR_MESSAGE + e.getMessage(), SERDES_ERROR);
        }
    }

    private static Object deserializeToBallerinaType(Object datum, Type ballerinaType, Schema schema) {
        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.BOOLEAN_TAG:
                return datum;

            case TypeTags.STRING_TAG:
                return StringUtils.fromString(datum.toString());

            case TypeTags.BYTE_TAG:
                return ((ByteBuffer) datum).get(0);

            case TypeTags.DECIMAL_TAG: {
                Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
                BigDecimal bigDecimal = decimalConversion.fromBytes((ByteBuffer) datum, schema,
                        LogicalTypes.decimal(34, 34));
                return ValueCreator.createDecimalValue(bigDecimal);
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }
}
