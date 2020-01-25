# DAML-on-QLDB Dependencies Sheet

## Programming Languages

| Name  | Description                                           | Provider        | URL                                                    |
| ----- | ----------------------------------------------------- | --------------- | ------------------------------------------------------ |
| Java  | Object-oriented language                              | Oracle, OpenJDK | http://www.oracle.com/java/, https://openjdk.java.net/ |
| Scala | Object-oriented language layer on Java and JavaScript | Scala           | http://www.scala-lang.org/                             |

## SDKs

| Name                     | Description                      | Provider       | Packages                                                | URL                                                          |
| ------------------------ | -------------------------------- | -------------- | ------------------------------------------------------- | ------------------------------------------------------------ |
| DAML                     | Digital Asset Modelling Language | Digital Asset  | `com.digitalasset.daml` `com.daml`                      | https://digitalasset.com/, https://daml.com/                 |
| AWS                      | Amazon Web Services              | AWS            | `com.amazonaws`                                         | https://aws.amazon.com/                                      |
| S3                       | Simple Storage Service           | AWS            | `com.amazonaws.services.s3`                             | https://aws.amazon.com/s3/                                   |
| QLDB                     | Quantum Ledger Database          | AWS            | `software.amazon.qldb`                                  | https://aws.amazon.com/qldb/                                 |
| Ion                      | JSON superset                    | AWS, FasterXML | `com.amazon.ion` `com.fasterxml.jackson.dataformat.ion` | http://amzn.github.io/ion-docs/, https://github.com/FasterXML/jackson-dataformats-binary/tree/master/ion |
| Protobuf                 | Data structure serialization     | Google         | `com.google.protobuf`                                   | https://developers.google.com/protocol-buffers               |
| ReactiveX 2 <sup>1</sup> | Reactive Extensions for JVM      | Coda Hale      | `io.reactivex`                                          | https://github.com/ReactiveX/RxJava                         |
| Metrics                  | Code metrics                     | Coda Hale      | `com.codahale.metrics`                                  | https://metrics.dropwizard.io/4.1.2/                         |

<sup>1</sup> Support for ReactiveX 2 ends on **December 31, 2020**. Consider migrating to ReactiveX 3.

## Toolchain

| Name    | Description                         | Provider | URL                               |
| ------- | ----------------------------------- | -------- | --------------------------------- |
| Docker  | App containerization                | Docker   | https://www.docker.com/           |
| Maven   | Software project management         | Apache   | http://maven.apache.org/          |
| Mockito | Java application testing tool suite | Mockito  | https://site.mockito.org/         |
| JUnit   | Java unit test tool                 | JUnit    | https://junit.org/junit5/         |
| bash    | Linux shell scripting language      | GNU      | http://www.gnu.org/software/bash/ |

## Markup Languages

| Name     | Description                 | Provider        | URL                                                          |
| -------- | --------------------------- | --------------- | ------------------------------------------------------------ |
| YAML     | YAML Ain’t Markup Language  | yaml.org        | https://yaml.org/spec/1.2/spec.html                          |
| Markdown | Lightweight markup language | Daring Fireball | https://daringfireball.net/projects/markdown/, https://www.markdownguide.org/ |