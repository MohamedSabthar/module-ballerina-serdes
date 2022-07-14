package io.ballerina.stdlib.serdes;


import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.math.BigDecimal;
import java.util.List;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;

/**
 * {@link io.ballerina.stdlib.serdes.MessageSerializer} provides generic functions for concrete messageTypes.
 */
public abstract class MessageSerializer {
    private final Builder dynamicMessageBuilder;
    private final Object anydata;
    private final BallerinaStructuredTypeMessageSerializer ballerinaStructuredTypeMessageSerializer;
    private String currentFieldName;

    public MessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                             BallerinaStructuredTypeMessageSerializer ballerinaStructuredTypeMessageSerializer) {
        this.dynamicMessageBuilder = dynamicMessageBuilder;
        this.anydata = anydata;
        this.ballerinaStructuredTypeMessageSerializer = ballerinaStructuredTypeMessageSerializer;
    }

    public abstract void setIntFieldValue(Object ballerinaInt);

    public abstract void setByteFieldValue(Integer ballerinaByte);

    public abstract void setFloatFieldValue(Object ballerinaFloat);

    public abstract void setDecimalFieldValue(BDecimal ballerinaDecimal);

    public abstract void setStringFieldValue(BString ballerinaString);

    public abstract void setBooleanFieldValue(Boolean ballerinaBoolean);

    public abstract void setNullFieldValue(Object ballerinaNil);

    public abstract void setRecordFieldValue(BMap<BString, Object> ballerinaRecord);

    public abstract void setMapFieldValue(BMap<BString, Object> ballerinaMap);

    public abstract void setTableFieldValue(BTable<?, ?> ballerinaTable);

    public abstract void setArrayFieldValue(BArray ballerinaArray);

    public abstract void setUnionFieldValue(Object unionValue);

    public abstract void setTupleFieldValue(BArray ballerinaTuple);

    public DynamicMessage generateDecimalValueMessage(Builder decimalMessageBuilder, Object decimal) {
        BigDecimal bigDecimal = ((BDecimal) decimal).decimalValue();
        Descriptor decimalSchema = decimalMessageBuilder.getDescriptorForType();

        FieldDescriptor scale = decimalSchema.findFieldByName(SCALE);
        FieldDescriptor precision = decimalSchema.findFieldByName(PRECISION);
        FieldDescriptor value = decimalSchema.findFieldByName(VALUE);

        decimalMessageBuilder.setField(scale, bigDecimal.scale());
        decimalMessageBuilder.setField(precision, bigDecimal.precision());
        decimalMessageBuilder.setField(value, bigDecimal.unscaledValue().toByteArray());
        return decimalMessageBuilder.build();
    }

    public void setMessageFieldValueInMessageBuilder(Object value) {
        FieldDescriptor fieldDescriptor = dynamicMessageBuilder.getDescriptorForType()
                .findFieldByName(getCurrentFieldName());
        getDynamicMessageBuilder().setField(fieldDescriptor, value);
    }

    public Builder getDynamicMessageBuilder() {
        return dynamicMessageBuilder;
    }

    public BallerinaStructuredTypeMessageSerializer getBallerinaStructuredTypeMessageSerializer() {
        return ballerinaStructuredTypeMessageSerializer;
    }

    public Object getAnydata() {
        return anydata;
    }

    public abstract List<MessageFieldData> getListOfMessageFieldData();

    public String getCurrentFieldName() {
        return currentFieldName;
    }

    public void setCurrentFieldName(String currentFieldName) {
        this.currentFieldName = currentFieldName;
    }
}
