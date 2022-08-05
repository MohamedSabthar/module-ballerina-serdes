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
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static io.ballerina.stdlib.serdes.Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SERIALIZATION_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.TYPE_MISMATCH_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Serializer class to create a byte array for a value.
 */
public class AvroSerializer {

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
        Schema schema = (Schema) ser.getNativeData(SCHEMA_NAME);
        ByteArrayOutputStream byteStream;
        try {
            byteStream = serializeAccordingToType(anydata, schema, bTypedesc.getDescribingType());
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (IllegalArgumentException e) {
            String errorMessage = SERIALIZATION_ERROR_MESSAGE + TYPE_MISMATCH_ERROR_MESSAGE;
            return createSerdesError(errorMessage, SERDES_ERROR);
        } catch (IOException e) {
            return createSerdesError(e.getMessage(), SERDES_ERROR);
        }
        return ValueCreator.createArrayValue(byteStream.toByteArray());
    }

    private static ByteArrayOutputStream serializeAccordingToType(Object anydata, Schema schema,
                                                                  Type ballerinaType) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG: {
                GenericDatumWriter<Long> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write((Long) anydata, encoder);
                break;
            }

            case TypeTags.FLOAT_TAG: {
                GenericDatumWriter<Double> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write((Double) anydata, encoder);
                break;
            }

            case TypeTags.BOOLEAN_TAG: {
                GenericDatumWriter<Boolean> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write((Boolean) anydata, encoder);
                break;
            }

            case TypeTags.BYTE_TAG: {
                byte[] bytes = new byte[]{((Integer) anydata).byteValue()};
                GenericDatumWriter<ByteBuffer> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write(ByteBuffer.wrap(bytes), encoder);
                break;
            }

            case TypeTags.STRING_TAG: {
                String string = ((BString) anydata).getValue();
                GenericDatumWriter<String> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write(string, encoder);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                BigDecimal bigDecimal = ((BDecimal) anydata).decimalValue();
                Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
                ByteBuffer decimalBuffer = decimalConversion.toBytes(bigDecimal, schema, LogicalTypes.decimal(34, 34));
                GenericDatumWriter<ByteBuffer> datumWriter = new GenericDatumWriter<>(schema);
                datumWriter.write(decimalBuffer, encoder);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }

        encoder.flush();
        out.close();
        return out;
    }
}
