package org.gooseman.hbasepaldemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gooseman.hbase.HBaseClient;
import org.gooseman.hbase.HBaseTable;
import org.gooseman.hbasepaldemo.model.Region;
import org.gooseman.hbasepaldemo.model.Sales;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
public class SalesController {

    private HBaseClient hBaseClient;

    @Autowired
    public SalesController(HBaseClient hBaseClient) {
        this.hBaseClient = hBaseClient;
    }

    @GetMapping("/salesByRegion")
    public List<Sales> getSalesByRegion(@RequestParam Region region) throws Exception {
        try(Connection hBaseConnection = hBaseClient.openConnection()) {
            try(HBaseTable<Sales> salesHBaseTable = new HBaseTable<>(Sales.class, hBaseConnection)) {
                Scan scan = new Scan();
                scan.setFilter(new PrefixFilter(Bytes.toBytes(region.getValue())));
                return salesHBaseTable.fetch(scan);
            }
        }
    }
}
