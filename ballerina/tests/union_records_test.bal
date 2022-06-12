import ballerina/test;

type P record {
    int a;
    float b;
};

type Q record {
    decimal c;
    string|int[] d;
};

type UnionRec P|Q;

@test:Config{}
public isolated function unionRecords() returns error? {
    UnionRec data = {
        c: 12e10,
        d: [1,2,3]
    };

    Proto3Schema ser = check new (UnionRec);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (UnionRec);
    UnionRec decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

type RecordArray Q[];

@test:Config{}
public isolated function RecordArrays() returns error? {
    RecordArray data = [{
        c: 12e10,
        d: [1,2,3]
    },
    {
        c: 12e10,
        d: [1,2,3]
    }];

    Proto3Schema ser = check new (RecordArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (RecordArray);
    RecordArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}