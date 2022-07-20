package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BString;

import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * {@link UnionMessageSerializer} class handles serialization of ballerina unions.
 */
public class UnionMessageSerializer extends MessageSerializer {


    public UnionMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        String stringValue = ballerinaString.getValue();
        FieldDescriptor fieldDescriptor = getCurrentFieldDescriptor();
        // Handle ballerina enum value
        if (fieldDescriptor == null) {
            // String value of ballerina enum used as protobuf field name
            fieldDescriptor = getDynamicMessageBuilder().getDescriptorForType().findFieldByName(stringValue);
        }
        getDynamicMessageBuilder().setField(fieldDescriptor, stringValue);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        setCurrentFieldValueInDynamicMessageBuilder(true);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        Object unionValue = getAnydata();
        Type type = TypeUtils.getType(unionValue);
        Map.Entry<String, Type> filedNameAndReferredType = UnionMessageType.mapMemberToFieldName(type);
        String fieldName = filedNameAndReferredType.getKey();
        Type referredType = filedNameAndReferredType.getValue();
        return List.of(new MessageFieldData(fieldName, unionValue, referredType));
    }
}
