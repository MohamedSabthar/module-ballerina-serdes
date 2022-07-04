package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
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
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;
import static io.ballerina.stdlib.serdes.SchemaGenerator.isNonReferencedRecordType;

/**
 * TableMessageType.
 */
public class TableMessageType extends MessageType {
    private final int tableEntryFieldNumber;

    public TableMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
        tableEntryFieldNumber = 1;
    }

    @Override
    void setIntField(IntegerType integerType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setByteField(ByteType byteType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setFloatField(FloatType floatType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setDecimalField(DecimalType decimalType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setStringField(StringType stringType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setBooleanField(BooleanType booleanType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setNullField(NullType nullType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setRecordField(RecordType recordType) {
        String nestedMessageName = isNonReferencedRecordType(recordType) ? RECORD_BUILDER : recordType.getName();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new RecordMessageType(recordType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                TABLE_ENTRY, tableEntryFieldNumber);
        messageBuilder.addField(valueField);
    }

    @Override
    void setMapField(MapType mapType) {
        String nestedMessageName = MAP_BUILDER;
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, messageBuilder);
        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        messageBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                TABLE_ENTRY, tableEntryFieldNumber);
        messageBuilder.addField(valueField);
    }

    @Override
    void setTableField(TableType tableType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setArrayField(ArrayType arrayType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setUnionField(UnionType unionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    void setTupleField(TupleType tupleType) {
        throw new UnsupportedOperationException();
    }
}
