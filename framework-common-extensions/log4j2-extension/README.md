<b>Description</b>

This library extends log4j2 library, applying necessary configurations to enforce the logging standards.
A console appender is defined with JsonLayout applied to it.

- Usage

Library functionality can be enabled just by defining a dependency. Any other logging library dependency other than the sl4j-api should be removed.
Without any code change this step will enable the general logging operations to be performed by the standards.

```xml
<dependency>
    <groupId>leap</groupId>
    <artifactId>data-log4j2-extension</artifactId>
    <version>0.0.1</version>
</dependency>
```

To customize the logging operations place a log4j2.properties file into project resources. Any log4j2 configuration can be  continued to used other than defining new appenders.


- Temporary Workaround for library access

Until the development Nexus access is granted to the developers, to produce a local copy of the library after cloning the libray perform a install operation.

```console
git clone {repositoy url}
mvn clean install
```