package org.gooseman.hbasepaldemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
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

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class SalesController {

    private Configuration hBaseConfiguration;

    @Autowired
    public SalesController(Configuration hBaseConfiguration) {
        this.hBaseConfiguration = hBaseConfiguration;
    }

    @GetMapping("/salesByRegion")
    public List<Sales> getSalesByRegion(@RequestParam Region region) throws Exception {
        try(HBaseClient hBaseClient = new HBaseClient(hBaseConfiguration)) {
            try(HBaseTable<Sales> salesHBaseTable = hBaseClient.getHBaseTable(Sales.class)) {
                Scan scan = new Scan();

                // search across distributed hbase regions
                // assuming the salt indicates the number of hbase regions
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,
                        salesHBaseTable.getHBaseInfo().getSaltStream()
                                .map(b -> new PrefixFilter(Bytes.add(b, Bytes.toBytes(region.getValue()))))
                                .collect(Collectors.toList()));

                scan.setFilter(filterList);
                return salesHBaseTable.fetch(scan);
            }
        }
    }

    @GetMapping("/salesBySalt")
    public List<Sales> getSalesBySalt(@RequestParam byte salt,  HttpServletResponse response) throws Exception {
        try(HBaseClient hBaseClient = new HBaseClient(hBaseConfiguration)) {
            try(HBaseTable<Sales> salesHBaseTable = hBaseClient.getHBaseTable(Sales.class)) {
                Scan scan = new Scan();
                scan.setFilter(new PrefixFilter(new byte[] { salt }));
                List<Sales> sales = salesHBaseTable.fetch(scan);
                response.addHeader("RowCount", String.valueOf(sales.size()));
                return sales;
            }
        }
    }
}
