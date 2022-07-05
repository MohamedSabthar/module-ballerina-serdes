package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
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
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.isNonReferencedRecordType;

/**
 * MapMessageType.
 */
public class MapMessageType extends MessageType {
    private final int keyFieldNumber;
    private final int valueFieldNumber;
    private final ProtobufMessageBuilder mapEntryBuilder;

    public MapMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                          BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);

        keyFieldNumber = 1;
        valueFieldNumber = 2;

        mapEntryBuilder = new ProtobufMessageBuilder(MAP_FIELD_ENTRY);
        ProtobufMessageFieldBuilder keyField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, STRING, KEY_NAME,
                keyFieldNumber);
        mapEntryBuilder.addField(keyField);
    }

    void addMapEntryBuilderToMapMessageBuilder() {
        getMessageBuilder().addNestedMessage(mapEntryBuilder);
        ProtobufMessageFieldBuilder mapEntryField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, MAP_FIELD_ENTRY,
                MAP_FIELD, 1);
        getMessageBuilder().addField(mapEntryField);
    }

    private ProtobufMessageFieldBuilder generateMessageField(int typeTag) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(typeTag);
        return new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, getCurrentFieldName(), valueFieldNumber);
    }

    @Override
    void setIntField(IntegerType integerType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.INT_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setByteField(ByteType byteType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.BYTE_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setFloatField(FloatType floatType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.FLOAT_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();

    }

    @Override
    void setDecimalField(DecimalType decimalType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.DECIMAL_TAG);
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, 1);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                2);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, 3);

        nestedMessageBuilder.addField(scaleField);
        nestedMessageBuilder.addField(precisionField);
        nestedMessageBuilder.addField(valueField);

        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.DECIMAL_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setStringField(StringType stringType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.STRING_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setBooleanField(BooleanType booleanType) {
        ProtobufMessageFieldBuilder messageField = generateMessageField(TypeTags.BOOLEAN_TAG);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setNullField(NullType nullType) {
        throw new UnsupportedOperationException();
    }


    @Override
    void setRecordField(RecordType recordType) {
        String nestedMessageName = isNonReferencedRecordType(recordType) ? RECORD_BUILDER : recordType.getName();
        boolean hasMessageDefinition = mapEntryBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
        // Check for cyclic reference in ballerina record
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                    mapEntryBuilder);

            // context switch 1
            var current = getMessageGenerator().getMessageType();
            getMessageGenerator().setMessageType(
                    new RecordMessageType(recordType, nestedMessageBuilder, getMessageGenerator()));

            nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
            mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

            // context switch 2
            getMessageGenerator().setMessageType(current);
        }

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setMapField(MapType mapType) {
        String nestedMessageName = MAP_BUILDER;
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, mapEntryBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(new MapMessageType(mapType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setTableField(TableType tableType) {
        String nestedMessageName = TABLE_BUILDER;
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, mapEntryBuilder);
        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TableMessageType(tableType, nestedMessageBuilder, getMessageGenerator()));

        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setArrayField(ArrayType arrayType) {

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new ArrayMessageType(arrayType, mapEntryBuilder, getMessageGenerator(), current));
        getMessageGenerator().getMessageType().setCurrentFieldName(getCurrentFieldName());
        getMessageGenerator().getMessageType().setCurrentFieldNumber(valueFieldNumber);
        getMessageGenerator().generateMessageDefinition();

        // context switch 2
        getMessageGenerator().setMessageType(current);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setUnionField(UnionType unionType) {
        String nestedMessageName = VALUE_NAME + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, mapEntryBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new UnionMessageType(unionType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }

    @Override
    void setTupleField(TupleType tupleType) {
        String nestedMessageName = TUPLE_BUILDER;

        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName, mapEntryBuilder);

        // context switch 1
        var current = getMessageGenerator().getMessageType();
        getMessageGenerator().setMessageType(
                new TupleMessageType(tupleType, nestedMessageBuilder, getMessageGenerator()));
        nestedMessageBuilder = getMessageGenerator().generateMessageDefinition();
        mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

        // context switch 2
        getMessageGenerator().setMessageType(current);

        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(messageField);
        addMapEntryBuilderToMapMessageBuilder();
    }
}
