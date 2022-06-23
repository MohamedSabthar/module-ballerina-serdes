package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

/**
 * TypeHandler defines the methods each concrete type handlers should implement.
 */
public interface TypeHandler {

    ProtobufMessageBuilder setNullField(int fieldNumber);

    ProtobufMessageBuilder setIntField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setByteField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setFloatField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setStringField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setBooleanField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setDecimalField(String fieldName, int fieldNumber);

    ProtobufMessageBuilder setUnionField(UnionType unionType, String fieldName, int fieldNumber);

    ProtobufMessageBuilder setArrayField(ArrayType arrayType, String fieldName, int fieldNumber);

    ProtobufMessageBuilder setRecordField(RecordType recordType, String fieldName, int fieldNumber);
}
