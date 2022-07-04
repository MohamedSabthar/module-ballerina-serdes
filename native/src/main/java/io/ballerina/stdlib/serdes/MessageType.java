package io.ballerina.stdlib.serdes;


import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

/**
 * MessageType.
 */
public abstract class MessageType {
    private final ProtobufMessageBuilder messageBuilder;
    private final BallerinaStructuredTypeMessageGenerator messageGenerator;
    private Type ballerinaType;
    private String currentFieldName;
    private int currentFieldNumber;

    public MessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                       BallerinaStructuredTypeMessageGenerator messageGenerator) {
        this.messageBuilder = messageBuilder;
        this.ballerinaType = ballerinaType;
        this.messageGenerator = messageGenerator;
        this.currentFieldNumber = 1;
    }

    public Type getBallerinaType() {
        return ballerinaType;
    }

    public void setBallerinaType(Type ballerinaType) {
        this.ballerinaType = ballerinaType;
    }

    public ProtobufMessageBuilder getMessageBuilder() {
        return messageBuilder;
    }

    public BallerinaStructuredTypeMessageGenerator getMessageGenerator() {
        return messageGenerator;
    }

    public String getCurrentFieldName() {
        return currentFieldName;
    }

    public void setCurrentFieldName(String fieldName) {
        this.currentFieldName = fieldName;
    }

    public int getCurrentFieldNumber() {
        return currentFieldNumber;
    }

    public void setCurrentFieldNumber(int currentFieldNumber) {
        this.currentFieldNumber = currentFieldNumber;
    }

    public void incrementFieldNumber() {
        ++currentFieldNumber;
    }

    void setEnumField(FiniteType finiteType) {
        throw new UnsupportedOperationException();
    }

    abstract void setIntField(IntegerType integerType);

    abstract void setByteField(ByteType byteType);

    abstract void setFloatField(FloatType floatType);

    abstract void setDecimalField(DecimalType decimalType);

    abstract void setStringField(StringType stringType);

    abstract void setBooleanField(BooleanType booleanType);

    abstract void setNullField(NullType nullType);

    abstract void setRecordField(RecordType recordType);

    abstract void setMapField(MapType mapType);

    abstract void setTableField(TableType tableType);

    abstract void setArrayField(ArrayType arrayType);

    abstract void setUnionField(UnionType unionType);

    abstract void setTupleField(TupleType tupleType);
}
