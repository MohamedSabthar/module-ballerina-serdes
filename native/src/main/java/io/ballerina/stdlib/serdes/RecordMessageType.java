package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link RecordMessageType} class generate protobuf message definition for ballerina records.
 */
public class RecordMessageType extends MessageType {

    public RecordMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                             BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String childMessageName = isAnonymousBallerinaRecord(recordType) ?
                getCurrentFieldName() + TYPE_SEPARATOR + RECORD_BUILDER : recordType.getName();
        addChildMessageDefinitionInMessageBuilder(childMessageName, recordType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, childMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        String childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + MAP_BUILDER;
        addChildMessageDefinitionInMessageBuilder(childMessageName, mapType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, childMessageName);
    }

    @Override
    public void setTableField(TableType tableType) {
        String childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TABLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(childMessageName, tableType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, childMessageName);
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

        // This call adds the value field in wrapped messageBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        addChildMessageDefinitionInMessageBuilder(childMessageName, unionType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, childMessageName);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(childMessageName, tupleType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, childMessageName);
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        RecordType recordType = (RecordType) getBallerinaType();
        Map<String, Field> recordFields = recordType.getFields();
        return recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName))
                .map(field -> Map.entry(field.getFieldName(), TypeUtils.getReferredType(field.getFieldType())))
                .collect(Collectors.toList());
    }
}
