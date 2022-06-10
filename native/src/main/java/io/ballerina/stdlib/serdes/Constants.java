package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

/**
 * Constant variable for serdes related operations.
 */
public class Constants {

    public static final String EMPTY_STRING = "";

    // Constants related to ballerina type
    public static final String STRING = "string";
    public static final String UNION = "union";
    public static final String BYTE = "byte";
    public static final String DECIMAL = "decimal";

    // Constants related to java type
    public static final String INTEGER = "Integer";

    // Constants related to protobuf schema
    public static final String SCHEMA_NAME = "schema";
    public static final String UNION_BUILDER_NAME = "UnionBuilder";
    public static final String UNION_FIELD_NAME = "unionField";
    public static final String ARRAY_BUILDER_NAME = "ArrayBuilder";
    public static final String DECIMAL_VALUE = "DecimalValue";
    public static final String ARRAY_FIELD_NAME = "arrayField";
    public static final String ATOMIC_FIELD_NAME = "atomicField";
    public static final String NULL_FIELD_NAME = "nullField";
    public static final String VALUE_SUFFIX = "Value";
    public static final String SCALE = "scale";
    public static final String PRECISION = "precision";
    public static final String VALUE = "value";

    public static final String SEPARATOR = "_";
    public static final String TYPE_SEPARATOR = "__";

    // Constants related to protobuf labels and types
    public static final String OPTIONAL_LABEL = "optional";
    public static final String REPEATED_LABEL = "repeated";
    public static final String BYTES = "bytes";
    public static final String UINT32 = "uint32";
    public static final String BOOL = "bool";

    // Constants related to error messages
    public static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
    public static final String DESERIALIZATION_ERROR_MESSAGE = "Failed to Deserialize data: ";
    public static final String SERIALIZATION_ERROR_MESSAGE = "Failed to Serialize data: ";
    public static final String TYPE_MISMATCH_ERROR_MESSAGE = "Type mismatch";
    public static final String SCHEMA_GENERATION_FAILURE = "Failed to generate schema: ";
    public static final BString BALLERINA_TYPEDESC_ATTRIBUTE_NAME = StringUtils.fromString("dataType");

}
