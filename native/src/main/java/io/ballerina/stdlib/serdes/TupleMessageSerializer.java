package io.ballerina.stdlib.serdes;

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;

import java.util.ArrayList;
import java.util.List;

import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_FIELD_NAME;

/**
 * RecordMessageSerializer.
 */
public class TupleMessageSerializer extends MessageSerializer {


    public TupleMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        BArray tuple = (BArray) getAnydata();
        List<MessageFieldData> messageFieldDataOfTupleElements = new ArrayList<>();
        List<Type> elementTypes = ((TupleType) tuple.getType()).getTupleTypes();
        for (int i = 0; i < tuple.size(); i++) {
            Object elementData = tuple.get(i);
            Type elementType = elementTypes.get(i);
            String fieldNameForElement = TUPLE_FIELD_NAME + SEPARATOR + (i + 1);
            messageFieldDataOfTupleElements.add(new MessageFieldData(fieldNameForElement, elementData, elementType));
        }
        return messageFieldDataOfTupleElements;
    }
}
