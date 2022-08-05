// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.import ballerina/test;

import ballerina/test;
import ballerina/io;

const TARGET_PROTO_FILE_DIRECTORY = "tests/target-protofiles/";

@test:Config { groups: ["proto3"] }
public isolated function testGenerateProtoFileForDecimalType() returns error? {
    string protofileName = "Decimal.proto";
    string expectedProtoFileContent = check io:fileReadString(TARGET_PROTO_FILE_DIRECTORY + protofileName);

    Proto3Schema ser = check new (decimal);
    check ser.generateProtoFile(protofileName);

    string protoFileContent = check io:fileReadString(protofileName);
    test:assertEquals(protoFileContent, expectedProtoFileContent);
}

type Contributor record {
    string username;
    byte[] img;
};

type Module record {
    string name;
    string|int id;
    int|() stars;
    Contributor[] contributors;
};

@test:Config { groups: ["proto3"] }
public isolated function testGenerateProtoFileForRecord() returns error? {
    string protofileName = "Module.proto";
    string expectedProtoFileContent = check io:fileReadString(TARGET_PROTO_FILE_DIRECTORY + protofileName);

    Proto3Schema ser = check new (Module);
    check ser.generateProtoFile(protofileName);

    string protoFileContent = check io:fileReadString(protofileName);
    test:assertEquals(protoFileContent, expectedProtoFileContent);
}
