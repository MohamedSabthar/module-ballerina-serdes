package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Serializer.generateMessageForTupleType;

/**
 * UnionMessageSerializer.
 */
public class UnionMessageSerializer extends MessageSerializer {


    public UnionMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
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
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        if (fieldDescriptor == null) {
            // enum value
            String fieldName = ballerinaString.getValue();
            fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType().findFieldByName(fieldName);
        }
        getDynamicMessageBuilder().setField(fieldDescriptor, ballerinaString.getValue());
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        setMessageFieldValueInMessageBuilder(ballerinaBoolean);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        getDynamicMessageBuilder().setField(fieldDescriptor, true);
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
        Builder recordMessageBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        var current = getBallerinaStructuredTypeMessageSerializer().getMessageSerializer();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(
                new MapMessageSerializer(recordMessageBuilder, ballerinaMap,
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
        getDynamicMessageBuilder().addRepeatedField(fieldDescriptor, nestedMessage);
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
        Builder tableBuilder = DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        DynamicMessage nestedMessage = generateMessageForTupleType(tableBuilder, ballerinaTuple).build();
        getDynamicMessageBuilder().setField(fieldDescriptor, nestedMessage);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        Object unionValue = getAnydata();
        Type type = TypeUtils.getType(unionValue);
        Map.Entry<String, Type> filedNameAndReferredType = MessageFieldNameGenerator.mapUnionMemberToMapEntry(type);
        String fieldName = filedNameAndReferredType.getKey();
        Type referredType = filedNameAndReferredType.getValue();
        return List.of(new MessageFieldData(fieldName, unionValue, referredType));
    }
}
