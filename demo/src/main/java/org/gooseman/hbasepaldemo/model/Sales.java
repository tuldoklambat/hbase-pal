package org.gooseman.hbasepaldemo.model;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.gooseman.hbase.HBase;
import org.gooseman.hbase.HBaseColumn;
import org.gooseman.hbase.HBaseRow;
import org.gooseman.hbasepaldemo.util.LocalDateConverter;
import org.gooseman.hbasepaldemo.util.RegionColumnConverter;
import org.gooseman.hbasepaldemo.util.RegionConverter;

import java.time.LocalDate;

@HBase(tableName = "Sales", salt = 32)
@Data
@EqualsAndHashCode(callSuper = false)
public class Sales extends HBaseRow {

    @HBaseColumn(converter = RegionColumnConverter.class)
    @CsvCustomBindByName(column = "Region", converter = RegionConverter.class)
    private Region region;

    @HBaseColumn
    @CsvBindByName(column = "Country")
    private String country;

    @HBaseColumn
    @CsvBindByName(column = "Item Type")
    private String itemType;

    @HBaseColumn
    @CsvBindByName(column = "Sales Channel")
    private String salesChannel;

    @HBaseColumn
    @CsvBindByName(column = "Order Priority")
    private String orderPriority;

    @HBaseColumn
    @CsvCustomBindByName(column = "Order Date", converter = LocalDateConverter.class)
    private LocalDate orderDate;

    @HBaseColumn
    @CsvBindByName(column = "Order ID")
    private int orderId;

    @HBaseColumn
    @CsvCustomBindByName(column = "Ship Date", converter = LocalDateConverter.class)
    private LocalDate shipDate;

    @HBaseColumn
    @CsvBindByName(column = "Units Sold")
    private int unitsSold;

    @HBaseColumn
    @CsvBindByName(column = "Unit Price")
    private double unitPrice;

    @HBaseColumn
    @CsvBindByName(column = "Unit Cost")
    private double unitCost;

    @HBaseColumn
    @CsvBindByName(column = "Total Revenue")
    private double totalRevenue;

    @HBaseColumn
    @CsvBindByName(column = "Total Cost")
    private double totalCost;

    @HBaseColumn
    @CsvBindByName(column = "Total Profit")
    private double totalProfit;

    @Override
    public byte[] getRowKey() {
        return Bytes.add(Bytes.toBytes(region.getValue()), Bytes.toBytes(orderId));
    }
}
