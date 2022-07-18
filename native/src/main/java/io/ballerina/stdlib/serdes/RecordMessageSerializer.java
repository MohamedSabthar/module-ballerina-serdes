package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RecordMessageSerializer.
 */
public class RecordMessageSerializer extends MessageSerializer {

    public RecordMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                   BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        throw new UnsupportedOperationException();
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
