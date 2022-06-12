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

import io.ballerina.stdlib.serdes.Constants;

import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static com.google.protobuf.Descriptors.FileDescriptor;

/**
 * Dynamically create and build a proto file.
 */
public class ProtobufFileBuilder {

    private final FileDescriptorProto.Builder fileDescProtoBuilder;
    private ProtobufMessageBuilder protobufMessage;

    public ProtobufFileBuilder() {
        fileDescProtoBuilder = FileDescriptorProto.newBuilder();
        fileDescProtoBuilder.setSyntax(Constants.PROTO3);
    }

    // Utmost one dynamic message schema added to the protobuf file
    public ProtobufFileBuilder addMessageType(ProtobufMessageBuilder protobufMessageBuilder) {
        fileDescProtoBuilder.addMessageType(protobufMessageBuilder.getProtobufMessage());
        protobufMessage = protobufMessageBuilder;
        return this;
    }

    public Descriptor build() throws DescriptorValidationException {
        FileDescriptor[] fileDescriptors = new FileDescriptor[]{};
        return FileDescriptor.buildFrom(fileDescProtoBuilder.build(), fileDescriptors).getMessageTypes().get(0);
    }

    @Override
    public String toString() {
        return Constants.SYNTAX + " = \"" + fileDescProtoBuilder.getSyntax() + "\";"
                + "\n\n" + protobufMessage.toString();
    }
}
