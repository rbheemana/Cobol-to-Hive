# Cobol-to-Hive
Serde for Cobol Layout to Hive table


#### Latest Updates

1. in `MainframeVBRecordReader.java`, on line 105, added cast as,
```java
 filePosition = (Seekable) cIn;
```
2. modified `pom.xml` so that we can compile it using maven as,
```sh
mvn package
```
3. Commented the util package so that compilation can be done without maven install.

4. Added support for PIC clause starting with v Ex: v9(6)

5. Fixed issue for signed decimals.

6. Added support to ignore fields based on java regex pattern supplied via 'cobol.field.ignorePattern'='JAVA_REGEX_PATTERN'

    ex: 'cobol.field.ignorePattern'='filler*'