package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;

/**
 * UnionMessageSerializer.
 */
public class TableMessageSerializer extends MessageSerializer {


    public TableMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setIntFieldValue(Object ballerinaInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFloatFieldValue(Object ballerinaFloat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        throw new UnsupportedOperationException();
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
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().generateMessage().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().addRepeatedField(fieldDescriptor, nestedMessage);
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
        DynamicMessage nestedMessage = getBallerinaStructuredTypeMessageSerializer().generateMessage().build();
        getBallerinaStructuredTypeMessageSerializer().setMessageSerializer(current);
        getDynamicMessageBuilder().addRepeatedField(fieldDescriptor, nestedMessage);
    }

    @Override
    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnionFieldValue(Object unionValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleFieldValue(BArray ballerinaTuple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        BTable<?, ?> table = (BTable<?, ?>) getAnydata();
        Type constrainedType = ((TableType) TypeUtils.getType(table)).getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        return table.values().stream().map(value -> new MessageFieldData(TABLE_ENTRY, value, referredConstrainedType))
                .collect(Collectors.toList());
    }
}
