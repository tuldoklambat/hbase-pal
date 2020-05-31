/*
 * Copyright (c) 2020 Gooseman Brothers (gooseman.brothers@gmail.com)
 * All rights reserved.
 *
 * THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
 * WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
 * MERCHANTABILITY OR NON-INFRINGEMENT.
 */

package org.gooseman.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseClient implements Closeable {

    private final Connection connection;

    public HBaseClient(Configuration configuration) throws IOException {
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    /**
     *
     * @param entityClass
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T extends HBaseRow> HBaseTable<T> getHBaseTable(Class<T> entityClass) throws Exception {
        return new HBaseTable<T>(entityClass).init(connection);
    }

    /**
     * Creates an HBase table based on the specified entity
     * @param entityClass
     * @param <T>
     * @return HBaseTable instance or null if the table already exists
     * @throws Exception
     */
    public <T extends HBaseRow> HBaseTable<T> createHBaseTable(Class<T> entityClass) throws Exception {
        try (HBaseTable<T> hBaseTable = new HBaseTable<>(entityClass)) {
            try(Admin admin = connection.getAdmin()) {
                if (admin.tableExists(hBaseTable.getHBaseInfo().getTableName())) {
                    return null;
                }

                T hBaseRowInstance = hBaseTable.createRowInstance();

                List<ColumnFamilyDescriptor> columnFamilyDescriptors = hBaseRowInstance.getHBaseColumnInfo().values()
                        .stream()
                        .map(c -> Bytes.toStringBinary(c.getBinFamily()))
                        .distinct()
                        .map(f -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(f)).build())
                        .collect(Collectors.toList());

                TableDescriptor tableDescriptor = TableDescriptorBuilder
                        .newBuilder(hBaseTable.getHBaseInfo().getTableName())
                        .setColumnFamilies(columnFamilyDescriptors)
                        .build();

                admin.createTable(tableDescriptor);

                return getHBaseTable(entityClass);
            }
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
