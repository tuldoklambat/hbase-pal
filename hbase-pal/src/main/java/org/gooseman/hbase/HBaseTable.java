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

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HBaseTable<T extends HBaseRow> implements Closeable {

    private Class<T> subClassType;
    private Table hBaseTable;
    private HBaseInfo hBaseInfo;

    protected HBaseTable(Class<T> subClassType) throws Exception {
        if (!subClassType.isAnnotationPresent(HBase.class)) {
            throw new Exception("Class must be decorated with @HBase");
        }
        this.hBaseInfo = new HBaseInfo(subClassType.getAnnotation(HBase.class));
        this.subClassType = subClassType;
    }

    protected HBaseTable<T> init(Connection connection) throws IOException {
        this.hBaseTable = connection.getTable(hBaseInfo.getTableName());
        return this;
    }

    @Override
    public void close() throws IOException {
        if (hBaseTable != null) {
            hBaseTable.close();
        }
    }

    /**
     * Gets the HBase information as specified in the {@link HBase} decoration.
     * @return
     */
    public HBaseInfo getHBaseInfo() {
        return hBaseInfo;
    }

    /**
     * Saves the list of {@link T} to the HBase table specified in the {@link HBase} decoration.
     * @param hBaseRows
     * @return
     */
    public void save(List<T> hBaseRows) throws Exception {
        List<Put> puts = new ArrayList<>();
        for(HBaseRow hBaseRow : hBaseRows) {
            puts.add(hBaseRow.getPut(hBaseInfo.getSalt()));
        }
        hBaseTable.put(puts);
    }

    /**
     * Fetch a list of {@link T}
     * @param scan
     * @return
     * @throws Exception
     */
    public List<T> fetch(Scan scan) throws Exception {
        try(ResultScanner resultScanner = hBaseTable.getScanner(scan)) {
            List<T> results = new ArrayList<>();
            for(Result result : resultScanner) {
                T instance = createRowInstance();
                instance.updateFieldsFromResult(result);
                results.add(instance);
            }
            return results;
        }
    }

    /**
     * Fetch a list of {@link T} given a start row and stop row
     * @param startRow
     * @param stopRow
     * @return
     * @throws Exception
     */
    public List<T> fetch(T startRow, T stopRow) throws Exception {
        return fetch(new Scan()
                .withStartRow(startRow.getKey(hBaseInfo.getSalt()))
                .withStartRow(stopRow.getKey(hBaseInfo.getSalt())));
    }

    /**
     *
     * @param scan
     * @return
     * @throws IOException
     */
    public Stream<T> stream(Scan scan) throws IOException {
        ResultScanner resultScanner = hBaseTable.getScanner(scan);
        Spliterator<Result> spliterator = Spliterators.spliteratorUnknownSize(resultScanner.iterator(),
                Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).map(r -> {
            try {
                T instance = createRowInstance();
                instance.updateFieldsFromResult(r);
                return instance;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).filter(Objects::nonNull);
    }

    /**
     *
     * @param hBaseRows
     * @throws Exception
     */
    public void refresh(List<T> hBaseRows) throws Exception {
        Map<String, T> index = hBaseRows.stream().collect(
                Collectors.toMap(k -> Bytes.toStringBinary(k.getKey(hBaseInfo.getSalt())), v -> v));
        List<Get> gets = hBaseRows.stream().map(r -> r.getGet(hBaseInfo.getSalt())).collect(Collectors.toList());
        for (Result result : hBaseTable.get(gets)) {
            T row = index.getOrDefault(Bytes.toStringBinary(result.getRow()), null);
            if (row != null) {
                row.updateFieldsFromResult(result);
            }
        }
    }

    public T createRowInstance() throws Exception {
        return subClassType.getDeclaredConstructor().newInstance();
    }
}
