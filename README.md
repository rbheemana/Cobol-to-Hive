# Cobol-to-Hive
Serde for Cobol Layout to Hive table

#### Changes as of 2/8/2018

1. in MainframeVBRecordReader.java`, on line 105, added cast as,
```java
 filePosition = (Seekable) cIn;
```
