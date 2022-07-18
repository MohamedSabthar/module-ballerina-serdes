package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link TupleMessageType} class generate protobuf message definition for ballerina tuples.
 */
public class TupleMessageType extends MessageType {
    public TupleMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = isAnonymousBallerinaRecord(recordType) ?
                getCurrentFieldName() + TYPE_SEPARATOR + RECORD_BUILDER : recordType.getName();
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, recordType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        String nestedMessageName = getCurrentFieldName() + SEPARATOR + MAP_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, mapType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setTableField(TableType tableType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TABLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, tableType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap mapEntryBuilder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, messageBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This call adds the value field in wrapped mapEntryBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, unionType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, tupleType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        TupleType tupleType = (TupleType) getBallerinaType();
        AtomicInteger elementIndex = new AtomicInteger(0);
        return tupleType.getTupleTypes().stream()
                .map(type -> Map.entry(TUPLE_FIELD_NAME + SEPARATOR + (elementIndex.incrementAndGet()),
                        TypeUtils.getReferredType(type))).collect(Collectors.toList());
    }
}
