package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;

/**
 * MapMessageSerializer.
 */
public class MapMessageSerializer extends MessageSerializer {
    private final Builder mapEntryBuilder;

    public MapMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
        FieldDescriptor mapFieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(MAP_FIELD);
        Descriptor mapEntryDescriptor = mapFieldDescriptor.getMessageType();
        mapEntryBuilder = DynamicMessage.newBuilder(mapEntryDescriptor);
    }

    @Override
    public void setIntFieldValue(Object ballerinaInt) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaInt);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        byte[] byteValue = new byte[]{ballerinaByte.byteValue()};
        mapEntryBuilder.setField(fieldDescriptor, byteValue);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setFloatFieldValue(Object ballerinaFloat) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaFloat);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Descriptor decimalMessageDescriptor = fieldDescriptor.getMessageType();
        Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalMessageDescriptor);
        DynamicMessage decimalMessage = generateDecimalValueMessage(decimalMessageBuilder, ballerinaDecimal);
        mapEntryBuilder.setField(fieldDescriptor, decimalMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaString.getValue());
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaBoolean);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRecordFieldValue(BMap<BString, Object> ballerinaRecord) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Builder recordMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new RecordMessageSerializer(recordMessageBuilder, ballerinaRecord,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        mapEntryBuilder.setField(fieldDescriptor, nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setMapFieldValue(BMap<BString, Object> ballerinaMap) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Builder recordMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new MapMessageSerializer(recordMessageBuilder, ballerinaMap,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        mapEntryBuilder.setField(fieldDescriptor, nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Builder tableMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new TableMessageSerializer(tableMessageBuilder, ballerinaTable,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        mapEntryBuilder.setField(fieldDescriptor, nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        setKeyFieldValueInMapEntryBuilder();
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        var childMessageSerializer = new ArrayMessageSerializer(mapEntryBuilder, ballerinaArray,
                getBallerinaStructuredTypeMessageSerializer());
        childMessageSerializer.setCurrentFieldName(VALUE_NAME);

        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(childMessageSerializer);
        getBallerinaStructuredTypeMessageSerializer().serialize();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setUnionFieldValue(Object unionValue) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        Builder unionMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new UnionMessageSerializer(unionMessageBuilder, unionValue,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        mapEntryBuilder.setField(fieldDescriptor, nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setTupleFieldValue(BArray ballerinaTuple) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        Builder tupleMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new TupleMessageSerializer(tupleMessageBuilder, ballerinaTuple,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        mapEntryBuilder.setField(fieldDescriptor, nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    private void setKeyFieldValueInMapEntryBuilder() {
        Descriptor mapEntryDescriptor = mapEntryBuilder.getDescriptorForType();
        FieldDescriptor keyFieldDescriptor = mapEntryDescriptor.findFieldByName(KEY_NAME);
        String mapKeyFieldName = getCurrentFieldName();
        mapEntryBuilder.setField(keyFieldDescriptor, mapKeyFieldName);
    }

    private void setValueFieldValueInMapEntryBuilder(Object value) {
        Descriptor mapEntryDescriptor = mapEntryBuilder.getDescriptorForType();
        FieldDescriptor valueFieldDescriptor = mapEntryDescriptor.findFieldByName(VALUE_NAME);
        mapEntryBuilder.setField(valueFieldDescriptor, value);
    }

    private void setMapFieldMessageValueInMessageBuilder() {
        FieldDescriptor mapField = getDynamicMessageBuilder().getDescriptorForType().findFieldByName(MAP_FIELD);
        getDynamicMessageBuilder().addRepeatedField(mapField, mapEntryBuilder.build());
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        @SuppressWarnings("unchecked") BMap<BString, Object> ballerinaMap = (BMap<BString, Object>) getAnydata();
        MapType mapType = (MapType) ballerinaMap.getType();
        Type constrainedType = mapType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        return ballerinaMap.entrySet().stream()
                .map(entry -> new MessageFieldData(entry.getKey().getValue(), entry.getValue(),
                        referredConstrainedType)).collect(Collectors.toList());
    }
}
