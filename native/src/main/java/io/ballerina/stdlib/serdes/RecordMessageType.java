package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * RecordMessageType.
 */
public class RecordMessageType extends MessageType {

    public RecordMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                             BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ?
                getCurrentFieldName() + TYPE_SEPARATOR + RECORD_BUILDER : recordType.getName();
        addNestedMessageDefinitionInMessageBuilder(recordType, nestedMessageName);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + MAP_BUILDER;
        addNestedMessageDefinitionInMessageBuilder(mapType, nestedMessageName);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setTableField(TableType tableType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TABLE_BUILDER;
        addNestedMessageDefinitionInMessageBuilder(tableType, nestedMessageName);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap messageBuilder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, messageBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This adds the value field in wrapped messageBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        addNestedMessageDefinitionInMessageBuilder(unionType, nestedMessageName);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        addNestedMessageDefinitionInMessageBuilder(tupleType, nestedMessageName);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }
}
