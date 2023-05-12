/*
 * Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.xlibb.serdes;

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;

import java.util.List;
import java.util.Map;

/**
 * {@link UnionMessageSerializer} class handles serialization of ballerina unions.
 */
public class UnionMessageSerializer extends MessageSerializer {


    public UnionMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setNullFieldValue(Object ballerinaNil) {
        setCurrentFieldValueInDynamicMessageBuilder(true);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        Object unionValue = getBallerinaStructureTypeValue();
        Type type = TypeUtils.getType(unionValue);
        Map.Entry<String, Type> filedNameAndReferredType = UnionMessageType.mapMemberToFieldName(type);
        String fieldName = filedNameAndReferredType.getKey();
        Type referredType = filedNameAndReferredType.getValue();
        return List.of(new MessageFieldData(fieldName, unionValue, referredType));
    }
}
