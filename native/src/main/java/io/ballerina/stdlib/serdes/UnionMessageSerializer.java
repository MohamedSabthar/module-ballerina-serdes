package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BString;

import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * UnionMessageSerializer.
 */
public class UnionMessageSerializer extends MessageSerializer {


    public UnionMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
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
    public void setNullFieldValue(Object ballerinaNil) {
        FieldDescriptor fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        getDynamicMessageBuilder().setField(fieldDescriptor, true);
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
