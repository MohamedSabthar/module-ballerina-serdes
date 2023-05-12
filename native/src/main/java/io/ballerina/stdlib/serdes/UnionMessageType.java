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

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.xlibb.serdes.protobuf.DataTypeMapper;
import io.xlibb.serdes.protobuf.ProtobufMessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.xlibb.serdes.Constants.ARRAY_FIELD_NAME;
import static io.xlibb.serdes.Constants.BOOL;
import static io.xlibb.serdes.Constants.EMPTY_STRING;
import static io.xlibb.serdes.Constants.MAP_MEMBER_NOT_YET_SUPPORTED;
import static io.xlibb.serdes.Constants.NIL;
import static io.xlibb.serdes.Constants.NULL_FIELD_NAME;
import static io.xlibb.serdes.Constants.OPTIONAL_LABEL;
import static io.xlibb.serdes.Constants.SEPARATOR;
import static io.xlibb.serdes.Constants.TABLE_MEMBER_NOT_YET_SUPPORTED;
import static io.xlibb.serdes.Constants.TUPLE_BUILDER;
import static io.xlibb.serdes.Constants.TYPE_SEPARATOR;
import static io.xlibb.serdes.Constants.UNION_FIELD_NAME;
import static io.xlibb.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.xlibb.serdes.Utils.SERDES_ERROR;
import static io.xlibb.serdes.Utils.createSerdesError;
import static io.xlibb.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link UnionMessageType} class generate protobuf message definition for ballerina union.
 */
public class UnionMessageType extends MessageType {
    public UnionMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    public static Map.Entry<String, Type> mapMemberToFieldName(Type memberType) {
        Type referredType = TypeUtils.getReferredType(memberType);
        String typeName = referredType.getName();

        if (referredType.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getArrayDimensions((ArrayType) referredType);
            typeName = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) referredType);
            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention + TYPE_SEPARATOR
                    + UNION_FIELD_NAME;
            return Map.entry(key, referredType);
        }

        // Handle enum members
        if (referredType.getTag() == TypeTags.FINITE_TYPE_TAG) {
            Type finiteValueType = TypeUtils.getType(referredType.getEmptyValue());
            typeName = TypeUtils.getReferredType(finiteValueType).getName();
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, referredType);
        }

        if (referredType.getTag() == TypeTags.RECORD_TYPE_TAG) {
            if (!Utils.isAnonymousBallerinaRecord(referredType)) {
                String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                return Map.entry(key, referredType);
            } else {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage((RecordType) referredType), SERDES_ERROR);
            }
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, referredType);
        }

        if (referredType.getTag() == TypeTags.TUPLE_TAG) {
            if (!typeName.equals(EMPTY_STRING)) {
                String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                return Map.entry(key, referredType);
            } else {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(memberType), SERDES_ERROR);
            }
        }

        if (referredType.getTag() == TypeTags.MAP_TAG) {
            // TODO: support map member
            throw createSerdesError(MAP_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        if (referredType.getTag() == TypeTags.TABLE_TAG) {
            // TODO: support table member
            throw createSerdesError(TABLE_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
    }

    @Override
    public void setEnumField(FiniteType finiteType) {
        Type referredMemberType = TypeUtils.getType(finiteType.getEmptyValue());
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredMemberType.getTag());
        if (referredMemberType.getTag() == TypeTags.DECIMAL_TAG) {
            ProtobufMessageBuilder decimalMessageDefinition = generateDecimalMessageDefinition();
            getMessageBuilder().addNestedMessage(decimalMessageDefinition);
        }
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    @Override
    public void setNullField(NullType nullType) {
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, BOOL);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        if (isAnonymousBallerinaRecord(recordType)) {
            throw createSerdesError(Utils.typeNotSupportedErrorMessage(recordType), SERDES_ERROR);
        }
        String nestedMessageName = recordType.getName();
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, recordType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        // TODO: add support to map type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + mapType.getName(), SERDES_ERROR);
    }

    @Override
    public void setTableField(TableType tableType) {
        // TODO: add support to table type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + tableType.getName(), SERDES_ERROR);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap existing message builder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, messageBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This call adds the value field in wrapped messageBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = tupleType.getName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, tupleType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public List<Map.Entry<String, Type>> getFieldNameAndBallerinaTypeEntryList() {
        UnionType unionType = (UnionType) getBallerinaType();
        return unionType.getMemberTypes().stream().map(UnionMessageType::mapMemberToFieldName)
                .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    }
}
