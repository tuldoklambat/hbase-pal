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
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public abstract class HBaseRow {

    private Map<String, HBaseColumnInfo> hBaseColumnInfoMap;

    protected HBaseRow() {
        hBaseColumnInfoMap = new HashMap<>();

        ReflectionUtils.doWithFields(getClass(), field -> {
            field.setAccessible(true);
            hBaseColumnInfoMap.put(field.getName(), new HBaseColumnInfo(field.getAnnotation(HBaseColumn.class), field));
        }, field -> field.isAnnotationPresent(HBaseColumn.class));
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
    public abstract byte[] getKey(int salt);

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
     * Sets the HBase column with the new binary array value.
     * @param hBaseColumnInfo
     * @return The value of the column
     */
    protected byte[] setColumnValue(HBaseColumnInfo hBaseColumnInfo) {
        return null;
    }

    /**
     * Sets the field value with the returned new value.
     * @param hBaseColumnInfo
     * @param result The HBase scan result
     * @return The new value of the field
     */
    protected Object setFieldValue(HBaseColumnInfo hBaseColumnInfo, Result result) {
        return null;
    }

    /**
     * Generates Put from the fields decorated with {@link HBaseColumn} class.
     * @param salt
     * @return
     * @throws Exception
     */
    @JsonIgnore
    Put getPut(int salt) throws Exception {
        Put put = new Put(getKey(salt));

        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            Field field = entry.getValue().getDecoratedField();
            byte[] newColumnValue = setColumnValue(entry.getValue());
            if (newColumnValue == null) {
                put.addColumn(entry.getValue().getBinFamily(), entry.getValue().getBinName(),
                        HBaseUtil.ToBytes(field.get(this), field.getType()));
            } else {
                put.addColumn(entry.getValue().getBinFamily(), entry.getValue().getBinName(),
                        newColumnValue);
            }
        }

        onAfterPut(put);

        return put;
    }

    /**
     * Generates Get from the fields decorated with {@link HBaseColumn} class.
     * @param salt
     * @return
     */
    @JsonIgnore
    Get getGet(int salt) {
        Get get = new Get(getKey(salt));

        hBaseColumnInfoMap.forEach((s, hBaseColumnInfo) -> get.addColumn(hBaseColumnInfo.getBinFamily(),
                hBaseColumnInfo.getBinName()));

        onAfterGet(get);

        return get;
    }

    /**
     * Update fields decorated with {@link HBaseColumn} with values from HBase {@link Result}
     * @param result
     */
    void updateFieldsFromResult(Result result) throws IllegalAccessException {
        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            HBaseColumnInfo ci = entry.getValue();
            if (result.containsColumn(ci.getBinFamily(), ci.getBinName())) {
                Field field = ci.getDecoratedField();
                byte[] value = result.getValue(ci.getBinFamily(), ci.getBinName());
                Object newValue = setFieldValue(ci, result);
                if (newValue == null) {
                    field.set(this, HBaseUtil.FromBytes(value, field.getType()));
                } else {
                    field.set(this, newValue);
                }
            }
        }
    }
}
