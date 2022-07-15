package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * RecordMessageSerializer.
 */
public class RecordMessageSerializer extends MessageSerializer {


    public RecordMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                   BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setIntFieldValue(Object ballerinaInt) {
        setMessageFieldValueInMessageBuilder(ballerinaInt);
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        byte[] byteValue = new byte[]{ballerinaByte.byteValue()};
        getDynamicMessageBuilder().setField(fieldDescriptor, byteValue);
    }

    @Override
    public void setFloatFieldValue(Object ballerinaFloat) {
        setMessageFieldValueInMessageBuilder(ballerinaFloat);
    }

    @Override
    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Descriptor decimalMessageDescriptor = fieldDescriptor.getMessageType();
        Builder decimalMessageBuilder = DynamicMessage.newBuilder(decimalMessageDescriptor);
        DynamicMessage decimalMessage = generateDecimalValueMessage(decimalMessageBuilder, ballerinaDecimal);
        getDynamicMessageBuilder().setField(fieldDescriptor, decimalMessage);
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        setMessageFieldValueInMessageBuilder(ballerinaString.getValue());
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        setMessageFieldValueInMessageBuilder(ballerinaBoolean);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRecordFieldValue(BMap<BString, Object> ballerinaRecord) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Builder recordMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new RecordMessageSerializer(recordMessageBuilder, ballerinaRecord,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public void setMapFieldValue(BMap<BString, Object> ballerinaMap) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Builder mapMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new MapMessageSerializer(mapMessageBuilder, ballerinaMap,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Builder tableMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new TableMessageSerializer(tableMessageBuilder, ballerinaTable,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        var childMessageSerializer = new ArrayMessageSerializer(getDynamicMessageBuilder(), ballerinaArray,
                getBallerinaStructuredTypeMessageSerializer());
        childMessageSerializer.setCurrentFieldName(getCurrentFieldName());

        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(childMessageSerializer);
        getBallerinaStructuredTypeMessageSerializer().serialize();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
    }

    @Override
    public void setUnionFieldValue(Object unionValue) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        Builder unionMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new UnionMessageSerializer(unionMessageBuilder, unionValue,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public void setTupleFieldValue(BArray ballerinaTuple) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        Builder tupleMessageBuilder = DynamicMessage.newBuilder(nestedSchema);
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new TupleMessageSerializer(tupleMessageBuilder, ballerinaTuple,
                        getBallerinaStructuredTypeMessageSerializer()));
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().serialize().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        @SuppressWarnings("unchecked") BMap<BString, Object> record = (BMap<BString, Object>) getAnydata();
        Map<String, Field> recordTypeFields = ((RecordType) record.getType()).getFields();
        return record.entrySet().stream().map(entry -> {
            String fieldName = entry.getKey().getValue();
            Object fieldValue = entry.getValue();
            Type fieldType = recordTypeFields.get(fieldName).getFieldType();
            Type referredType = TypeUtils.getReferredType(fieldType);
            return new MessageFieldData(fieldName, fieldValue, referredType);
        }).collect(Collectors.toList());
    }
}
