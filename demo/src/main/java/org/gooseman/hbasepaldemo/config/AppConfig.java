package org.gooseman.hbasepaldemo.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Bean
    public org.apache.hadoop.conf.Configuration getHBaseConfiguration() {
        org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hBaseConfig.set("hbase.zookeeper.quorom", "127.0.0.1");
        return hBaseConfig;
    }
}
