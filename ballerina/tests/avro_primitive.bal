import ballerina/test;

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveIntAvro() returns error? {
    int value = 666;

    AvroSchema ser = check new (int);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (int);
    int decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveBooleanAvro() returns error? {
    boolean value = false;

    AvroSchema ser = check new (boolean);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (boolean);
    boolean decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveFloatAvro() returns error? {
    float value = 6.666;

    AvroSchema ser = check new (float);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (float);
    float decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveStringAvro() returns error? {
    string value = "module-ballerina-serdes";

    AvroSchema ser = check new (string);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (string);
    string decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveByteAvro() returns error? {
    byte value = 3;

    AvroSchema ser = check new (byte);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (byte);
    byte decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config { groups: ["avro"] }
public isolated function testPrimitiveDecimalAvro() returns error? {
    decimal value = 1e-10;

    AvroSchema ser = check new (decimal);
    byte[] encoded = check ser.serialize(value);

    AvroSchema des = check new (decimal);
    decimal decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}
