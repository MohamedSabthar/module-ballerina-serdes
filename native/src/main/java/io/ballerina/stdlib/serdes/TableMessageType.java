package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link TableMessageType} class generate protobuf message definition for ballerina tables.
 */
public class TableMessageType extends MessageType {
    private static final int tableEntryFieldNumber = 1;

    public TableMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setIntField(IntegerType integerType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setByteField(ByteType byteType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFloatField(FloatType floatType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStringField(StringType stringType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ? RECORD_BUILDER : recordType.getName();
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, recordType);
        addEntryFieldInMessageBuilder(nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        String nestedMessageName = MAP_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, mapType);
        addEntryFieldInMessageBuilder(nestedMessageName);
    }

    private void addEntryFieldInMessageBuilder(String nestedMessageName) {
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName,
                getCurrentFieldName(), tableEntryFieldNumber);
        getMessageBuilder().addField(valueField);
    }

    @Override
    public void setTableField(TableType tableType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnionField(UnionType unionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        TableType tableType = (TableType) getBallerinaType();
        return List.of(Map.entry(TABLE_ENTRY, TypeUtils.getReferredType(tableType.getConstrainedType())));
    }
}
