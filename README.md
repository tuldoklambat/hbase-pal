# HBase PAL
HBase PAL or POJO Abstraction Layer is a library that abstracts retrieval and persistence of POJOs to HBase through the use of annotation that maps a POJO class to an HBase table and its fields to HBase columns.
### Examples:
Sample Sales entity decorated with HBase and HBaseColumn
```java
@HBase(tableName = "Sales")
@Data
public class Sales extends HBaseRow {

    @HBaseColumn
    private String region;
    @HBaseColumn
    private String country;
    @HBaseColumn
    private String itemType;
    @HBaseColumn
    private LocalDate orderDate;
    @HBaseColumn
    private int orderId;
    @HBaseColumn
    private int unitsSold;
    @HBaseColumn
    private double unitPrice;
    @HBaseColumn
    private double unitCost;
    
    @Override
    public byte[] getKey(int salt) {
        return Bytes.toBytes(orderId);
    }
}
```
Sample code to retrieve a list of Sales from HBase
```java
try(Connection hBaseConnection = hBaseClient.openConnection()) {
    try(HBaseTable<Sales> salesHBaseTable = new HBaseTable<>(Sales.class, hBaseConnection)) {
        Scan scan = new Scan();
        List<Sales> sales = salesHBaseTable.fetch(scan);
    }
}
```
## Features:
#### Retrieval/persistence
```java
// HBaseTable
List<T> fetch(Scan scan);   // get a list of type T from HBase table
Steam<T> stream(Scan scan); // get a stream of type T from HBase table
void refresh(List<T> rows); // refresh a list of type T with data from HBase table
void save(List<T> rows);    // saves a list of type T to HBase table
```
#### Row Key Salting
To support enterprise requirements to distribute rows across regions to avoid hot-spotting.
```java
@HBase(tableName = "Sales", salt = 8)
.
.
.
@Override
public byte[] getKey(int salt) {
    return Bytes.add(Bytes.toBytes(HBaseUtil.getSaltedHashValue(orderId, salt)),
            Bytes.toBytes(orderId));
}

```
#### Custom column and family name mapping 
```java
@HbaseColumn(name = "columnName", family = "f")
```
#### Get and put enrichment
For additional columns you want to push/pull from HBase without mapping them to fields.
```java
// HBaseRow
void onAfterGet(Get get);
void onAfterPut(Put put);
```
#### Custom field processing
For cases where you have a field type that cannot be converted to and from a binary array e.g. enumerations.
```java
// HBaseRow
byte[] setColumnValue(HBaseColumnInfo hBaseColumnInfo);
Object setFieldValue(HBaseColumnInfo hBaseColumnInfo, Result result);
```
### Demo Code
The demo code assumes you have HBase installed on your local host or via Docker (I used <a href="https://github.com/dajobe/hbase-docker" target="_blank">dajobe/hbase</a> personally, you can pull it directly from Docker Hub).  I used IntelliJ for IDE, open up the **demo** project, it references the **hbase-pal** module.

Once you've setup your HBase, you need to run the demo application initially with a program argument of **--spring.profiles.active=init** for it to go through with the Sales table creation and data population (remember to remove the argument on subsequent runs). The sample includes a Swagger UI so you can test it by invoking <a href="http://localhost:8888/swagger-ui.html" target="_blank">http://localhost:8888/swagger-ui.html</a> from your browser.