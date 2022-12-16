Ballerina SerDes Library
===================

[![Build](https://github.com/ballerina-platform/module-ballerina-serdes/workflows/Build/badge.svg)](https://github.com/ballerina-platform/module-ballerina-serdes/actions?query=workflow%3ABuild)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerina-serdes/branch/main/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerina-serdes)
[![Trivy](https://github.com/ballerina-platform/module-ballerina-serdes/actions/workflows/trivy-scan.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerina-serdes/actions/workflows/trivy-scan.yml)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerina-serdes/actions/workflows/build-with-bal-test-native.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerina-serdes/actions/workflows/build-with-bal-test-native.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerina-serdes.svg)](https://github.com/ballerina-platform/module-ballerina-serdes/commits/main)
[![Github issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-standard-library/module/serdes.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-standard-library/labels/module%2Fserdes)


This library provides APIs for serializing and deserializing subtypes of Ballerina anydata type.

### Proto3Schema

An instance of the `serdes:Proto3Schema` class is used to serialize and deserialize ballerina values using protocol buffers.

#### Create a `serdes:Proto3Schema` object

```ballerina
// Define a type which is a subtype of anydata.
type Student record {
    int id;
    string name;
    decimal fees;
};

// Create a schema object by passing the type.
serdes:Proto3Schema schema = check new (Student);
```
While instantiation of this object, an underlying proto3 schema generated for the provided typedesc.

#### Serialization

```ballerina
Student student = {
    id: 7894,
    name: "Liam",
    fees: 24999.99
};

// Serialize the record value to bytes.
byte[] bytes = check schema.serialize(student);
```
A value having the same type as the provided type can be serialized by invoking the `serialize` method on the previously instantiated `serdes:Proto3Schema` object. The underlying implementation uses the previously generated proto3 schema to serialize the provided value.

#### Deserialization

```ballerina
type Student record {
    int id;
    string name;
    decimal fees;
};

byte[] bytes = readSerializedDataToByteArray();
serdes:Proto3Schema schema = check new (Student);

// Deserialize the record value from bytes.
Student student = check schema.deserialize(bytes);
```
The serialized value (`byte[]`) can be again deserialized by invoking the `deserialize` method on the instantiated `serdes:Proto3Schema` object. The underlying implementation uses the previously generated proto3 schema and deserializes the provided `byte[]`. As the result of deserialization the method returns the ballerina value with the type represented by the typedesc value provided during the `serdes:Proto3Schema` object instantiation.

## Issues and Projects

The **Issues** and **Projects** tabs are disabled for this repository as this is part of the Ballerina Standard Library. To report bugs, request new features, start new discussions, view project boards, etc., go to the Ballerina Standard Library [parent repository](https://github.com/ballerina-platform/ballerina-standard-library).

This repository contains only the source code of the package.

## Building from the Source

### Setting Up the Prerequisites

1. Download and install Java SE Development Kit (JDK) version 11 (from one of the following locations).
    * [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)

    * [OpenJDK](https://adoptium.net/)

      > **Note:** Set the JAVA_HOME environment variable to the path name of the directory into which you installed JDK.

2. Export your Github Personal access token with the read package permissions as follows.

              export packageUser=<Username>
              export packagePAT=<Personal access token>

### Building the Source

Execute the commands below to build from source.

1. To build the package:

        ./gradlew clean build

2. To run the integration tests:

        ./gradlew clean test

3. To build the package without the tests:

        ./gradlew clean build -x test

4. To debug the tests:

        ./gradlew clean build -Pdebug=<port>

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Discuss about code changes of the Ballerina project in [ballerina-dev@googlegroups.com](mailto:ballerina-dev@googlegroups.com).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
