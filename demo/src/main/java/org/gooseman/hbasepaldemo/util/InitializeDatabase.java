package org.gooseman.hbasepaldemo.util;

import com.opencsv.bean.CsvToBeanBuilder;
import org.gooseman.hbase.HBaseClient;
import org.gooseman.hbase.HBaseTable;
import org.gooseman.hbasepaldemo.model.Sales;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

@Profile("init")
@Component
public class InitializeDatabase {

    private org.apache.hadoop.conf.Configuration configuration;

    @Autowired
    public InitializeDatabase(org.apache.hadoop.conf.Configuration configuration) throws Exception {
        try (HBaseClient hBaseClient = new HBaseClient(configuration)) {
            try(HBaseTable<Sales> salesHBaseTable = hBaseClient.createHBaseTable(Sales.class)) {
                if (salesHBaseTable != null) {
                    populateTable(salesHBaseTable);
                }
            }
        }
    }

    private List<Sales> getCSVSalesData() throws IOException {
        CsvToBeanBuilder<Sales> beanBuilder = new CsvToBeanBuilder<>(
                new InputStreamReader(getClass().getClassLoader().getResource("1000_Sales_Records.csv")
                        .openStream()));
        return beanBuilder.withType(Sales.class).build().parse();
    }

    private void populateTable(HBaseTable<Sales> salesHBaseTable) throws Exception {
        List<Sales> salesData = getCSVSalesData();
        salesHBaseTable.save(salesData);
    }
}
