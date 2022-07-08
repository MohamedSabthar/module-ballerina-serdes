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
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.isNonReferencedRecordType;

/**
 * MapMessageType.
 */
public class MapMessageType extends MessageType {
    private static final int keyFieldNumber = 1;
    private static final int valueFieldNumber = 2;
    private final ProtobufMessageBuilder mapEntryBuilder;

    public MapMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                          BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
        mapEntryBuilder = new ProtobufMessageBuilder(MAP_FIELD_ENTRY);
    }

    @Override
    public void setIntField(IntegerType integerType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(integerType.getTag());
        buildMapMessageDefinition(protoType);
    }

    @Override
    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(byteType.getTag());
        buildMapMessageDefinition(protoType);
    }

    @Override
    public void setFloatField(FloatType floatType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(floatType.getTag());
        buildMapMessageDefinition(protoType);
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder decimalMessageDefinition = generateDecimalMessageDefinition();
        buildMapMessageDefinitionWithNestedMessage(decimalMessageDefinition);
    }

    @Override
    public void setStringField(StringType stringType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(stringType.getTag());
        buildMapMessageDefinition(protoType);
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(booleanType.getTag());
        buildMapMessageDefinition(protoType);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String nestedMessageName = isNonReferencedRecordType(recordType) ? RECORD_BUILDER : recordType.getName();
        boolean hasMessageDefinition = mapEntryBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        // Check for cyclic reference in ballerina record
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                    mapEntryBuilder);
            MessageType childMessageType = new RecordMessageType(recordType, nestedMessageBuilder,
                    getMessageGenerator());
            ProtobufMessageBuilder childMessageDefinition = getNestedMessageDefinition(childMessageType);
            mapEntryBuilder.addNestedMessage(childMessageDefinition);
        }

        buildMapMessageDefinition(nestedMessageName);
    }

    private void buildMapMessageDefinition(String valueFieldType) {
        setKeyFieldInMapEntryBuilder();
        setValueFieldInMapEntryBuilder(valueFieldType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setMapField(MapType mapType) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(MAP_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        buildMapMessageDefinitionWithNestedMessage(nestedMessageDefinition);
    }

    @Override
    public void setTableField(TableType tableType) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(TABLE_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new TableMessageType(tableType, nestedMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        buildMapMessageDefinitionWithNestedMessage(nestedMessageDefinition);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        setKeyFieldInMapEntryBuilder();

        MessageType parentMessageType = getMessageGenerator().getMessageType();
        MessageType childMessageType = new ArrayMessageType(arrayType, mapEntryBuilder, getMessageGenerator(),
                parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(valueFieldNumber);

        // switch to child message type
        getMessageGenerator().setMessageType(childMessageType);
        getMessageGenerator().generateMessageDefinition();

        // switch back to parent message type
        getMessageGenerator().setMessageType(parentMessageType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String nestedMessageName = VALUE_NAME + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, mapEntryBuilder);
        MessageType childMessageType = new UnionMessageType(unionType, nestedMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        buildMapMessageDefinitionWithNestedMessage(nestedMessageDefinition);
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(TUPLE_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
        buildMapMessageDefinitionWithNestedMessage(nestedMessageDefinition);
    }

    private ProtobufMessageBuilder getNestedMessageDefinition(MessageType childMessageType) {
        MessageType parentMessageType = getMessageGenerator().getMessageType();
        // switch to child message type
        getMessageGenerator().setMessageType(childMessageType);
        ProtobufMessageBuilder childMessageBuilder = getMessageGenerator().generateMessageDefinition();
        // switch back to parent message type
        getMessageGenerator().setMessageType(parentMessageType);
        return childMessageBuilder;
    }

    private void buildMapMessageDefinitionWithNestedMessage(ProtobufMessageBuilder childMessageBuilder) {
        setKeyFieldInMapEntryBuilder();
        mapEntryBuilder.addNestedMessage(childMessageBuilder);
        setValueFieldInMapEntryBuilder(childMessageBuilder.getName());
        addMapEntryFieldInMessageBuilder();
    }

    private void setKeyFieldInMapEntryBuilder() {
        ProtobufMessageFieldBuilder keyField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, STRING, KEY_NAME,
                keyFieldNumber);
        mapEntryBuilder.addField(keyField);
    }

    private void setValueFieldInMapEntryBuilder(String fieldType) {
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, fieldType,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(valueField);
    }

    void addMapEntryFieldInMessageBuilder() {
        getMessageBuilder().addNestedMessage(mapEntryBuilder);
        final int mapEntryNumber = 1;
        ProtobufMessageFieldBuilder mapEntryField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, MAP_FIELD_ENTRY,
                MAP_FIELD, mapEntryNumber);
        getMessageBuilder().addField(mapEntryField);
    }
}
