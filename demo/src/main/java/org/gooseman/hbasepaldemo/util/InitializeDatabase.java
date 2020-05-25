package org.gooseman.hbasepaldemo.util;

import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gooseman.hbase.HBaseClient;
import org.gooseman.hbase.HBaseTable;
import org.gooseman.hbasepaldemo.model.Sales;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

@Profile("init")
@Component
public class InitializeDatabase {

    private HBaseClient hBaseClient;

    public InitializeDatabase(HBaseClient hBaseClient) throws Exception {
        this.hBaseClient = hBaseClient;

        createTable();
        populateTable();
    }

    private List<Sales> getCSVSalesData() throws IOException {
        CsvToBeanBuilder<Sales> beanBuilder = new CsvToBeanBuilder<>(
                new InputStreamReader(getClass().getClassLoader().getResource("1000_Sales_Records.csv").openStream()));
        return beanBuilder.withType(Sales.class).build().parse();
    }

    private void createTable() throws Exception {
        try(Connection hBaseConnection = hBaseClient.openConnection()) {
            try(HBaseTable<Sales> salesHBaseTable = new HBaseTable<>(Sales.class, hBaseConnection)) {
                Sales salesObject = salesHBaseTable.createRowInstance();
                List<ColumnFamilyDescriptor> columnFamilyDescriptors = salesObject.getHBaseColumnInfo().values().stream()
                        .map(c -> Bytes.toStringBinary(c.getBinFamily()))
                        .distinct()
                        .map(f -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(f)).build())
                        .collect(Collectors.toList());

                TableDescriptor tableDescriptor = TableDescriptorBuilder
                        .newBuilder(salesHBaseTable.getHBaseInfo().getTableName())
                        .setColumnFamilies(columnFamilyDescriptors)
                        .build();

                try(Admin admin = hBaseConnection.getAdmin()) {
                    if (!admin.tableExists(salesHBaseTable.getHBaseInfo().getTableName())) {
                        admin.createTable(tableDescriptor);
                    }
                }
            }
        }
    }

    private void populateTable() throws Exception {
        try(Connection hBaseConnection = hBaseClient.openConnection()) {
            try (HBaseTable<Sales> salesHBaseTable = new HBaseTable<>(Sales.class, hBaseConnection);) {
                List<Sales> salesData = getCSVSalesData();
                salesHBaseTable.save(salesData);
            }
        }
    }
}
