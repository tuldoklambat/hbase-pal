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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public abstract class HBaseRow {

    private final Map<String, HBaseColumnInfo> hBaseColumnInfoMap;

    protected HBaseRow()  {
        hBaseColumnInfoMap = new HashMap<>();
        for(Field field : getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(HBaseColumn.class)) {
                field.setAccessible(true);
                hBaseColumnInfoMap.put(field.getName(),
                        new HBaseColumnInfo(field.getAnnotation(HBaseColumn.class), field));
            }
        }
    }

    /**
     * Returns the object's field and HBase column mapping information
     * @return
     */
    @JsonIgnore
    public final Map<String, HBaseColumnInfo> getHBaseColumnInfo() {
        return hBaseColumnInfoMap;
    }

    @JsonIgnore
    public abstract byte[] getKey();

    /**
     * Called after generating the Put from the fields decorated with @link org.gooseman.hbase.HBaseColumn} class.
     * @param put
     */
    protected void onAfterPut(Put put) {
    }

    /**
     * Called after generating the Get from the fields decorated with @link org.gooseman.hbase.HBaseColumn} class.
     * @param get
     */
    protected void onAfterGet(Get get) {
    }

    /**
     * Allows for further processing of the result from HBase
     * @param result
     */
    protected void onResult(Result result) {
    }

    /**
     * Generates Put from the fields decorated with {@link HBaseColumn} class.
     * @return
     * @throws Exception
     */
    @JsonIgnore
    Put getPut(short salt) throws Exception {
        Put put = new Put(getSaltedKey(salt));
        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            Field field = entry.getValue().getDecoratedField();
            byte[] binValue = entry.getValue().getHBaseColumnConverter().ToBytes(field.get(this), field.getType());
            if (binValue != null) {
                put.addColumn(entry.getValue().getBinFamily(), entry.getValue().getBinName(), binValue);
            }
        }
        onAfterPut(put);
        return put;
    }

    /**
     * Generates Get from the fields decorated with {@link HBaseColumn} class.
     * @return
     */
    @JsonIgnore
    Get getGet(short salt) {
        Get get = new Get(getSaltedKey(salt));
        hBaseColumnInfoMap.forEach((s, hBaseColumnInfo) -> get.addColumn(hBaseColumnInfo.getBinFamily(),
                hBaseColumnInfo.getBinName()));
        onAfterGet(get);
        return get;
    }

    /**
     * Update fields decorated with {@link HBaseColumn} with values from HBase {@link Result}
     * @param result
     */
    void updateFieldsFromResult(Result result) throws IllegalAccessException, NoSuchMethodException,
            InstantiationException, InvocationTargetException {
        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            HBaseColumnInfo ci = entry.getValue();
            if (result.containsColumn(ci.getBinFamily(), ci.getBinName())) {
                Field field = ci.getDecoratedField();
                byte[] binValue = result.getValue(ci.getBinFamily(), ci.getBinName());
                field.set(this, ci.getHBaseColumnConverter().FromBytes(binValue, field.getType()));
            }
        }
        onResult(result);
    }

    byte[] getSaltedKey(short salt) {
        byte[] key = getKey();
        return salt > 0
                ? Bytes.add(Bytes.toBytes(HBaseUtil.getSaltedHashValue(key, salt)), key)
                : key;
    }
}
