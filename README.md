# Cobol-to-Hive
Serde for Cobol Layout to Hive table

#### Changes as of 2/8/2018

1. in `MainframeVBRecordReader.java`, on line 105, added cast as,
```java
 filePosition = (Seekable) cIn;
```
2. modified `pom.xml` so that we can compile it using maven as,
```sh
mvn package
```
3. Need to add sqoop jar manually as,
```sh
mvn install:install-file \
-Dfile=/location/of/sqoop.jar \
-DgroupId=org.apache.sqoop \
-DartifactId=sqoop \
-Dversion=1.4.6 \
-Dpackaging=jar
```
