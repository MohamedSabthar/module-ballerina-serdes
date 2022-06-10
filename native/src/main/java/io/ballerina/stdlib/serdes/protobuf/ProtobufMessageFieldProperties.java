/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.serdes.protobuf;

import java.util.HashMap;
import java.util.Map;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;

/**
 * Provides String to protobuf field properties mapping.
 * Properties include protobuf field types and labels.
 */
public class ProtobufMessageFieldProperties {

    private static final Map<String, Type> fieldTypes = new HashMap<>();
    private static final Map<String, Label> fieldLabels = new HashMap<>();

    static {
        fieldTypes.put("double", Type.TYPE_DOUBLE);
        fieldTypes.put("float", Type.TYPE_FLOAT);
        fieldTypes.put("int32", Type.TYPE_INT32);
        fieldTypes.put("int64", Type.TYPE_INT64);
        fieldTypes.put("uint32", Type.TYPE_UINT32);
        fieldTypes.put("uint64", Type.TYPE_UINT64);
        fieldTypes.put("sint32", Type.TYPE_SINT32);
        fieldTypes.put("sint64", Type.TYPE_SINT64);
        fieldTypes.put("fixed32", Type.TYPE_FIXED32);
        fieldTypes.put("fixed64", Type.TYPE_FIXED64);
        fieldTypes.put("sfixed32", Type.TYPE_SFIXED32);
        fieldTypes.put("sfixed64", Type.TYPE_SFIXED64);
        fieldTypes.put("bool", Type.TYPE_BOOL);
        fieldTypes.put("string", Type.TYPE_STRING);
        fieldTypes.put("bytes", Type.TYPE_BYTES);

        fieldLabels.put("optional", Label.LABEL_OPTIONAL);
        fieldLabels.put("required", Label.LABEL_REQUIRED);
        fieldLabels.put("repeated", Label.LABEL_REPEATED);
    }

    public static Type getFieldType(String type) {
        return fieldTypes.get(type);
    }

    public static Label getFieldLabel(String label) {
        return fieldLabels.get(label);
    }
}
