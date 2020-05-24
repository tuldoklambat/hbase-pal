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

import static org.gooseman.hbase.HBaseUtil.*;

public abstract class HBaseRow {

    private Map<String, HBaseColumnInfo> hBaseColumnInfoMap;

    protected HBaseRow() {
        hBaseColumnInfoMap = new HashMap<>();

        ReflectionUtils.doWithFields(getClass(), field -> {
            field.setAccessible(true);
            hBaseColumnInfoMap.put(field.getName(), new HBaseColumnInfo(field.getAnnotation(HBaseColumn.class), field));
        }, field -> field.isAnnotationPresent(HBaseColumn.class));
    }

    @JsonIgnore
    public abstract byte[] getKey(int salt);

    /**
     * Called after generating the Put from the fields decorated with @link org.gooseman.hbase.HBaseColumn} class.
     * @param put
     * @param hBaseColumnInfoMap
     */
    public void onAfterPut(Put put, final Map<String, HBaseColumnInfo> hBaseColumnInfoMap) {
    }

    /**
     * Called after generating the Get from the fields decorated with @link org.gooseman.hbase.HBaseColumn} class.
     * @param get
     * @param hBaseColumnInfoMap
     */
    public void onAfterGet(Get get, final Map<String, HBaseColumnInfo> hBaseColumnInfoMap) {
    }

    /**
     * Generates Put from the fields decorated with {@link org.gooseman.hbase.HBaseColumn} class.
     * @param salt
     * @return
     * @throws Exception
     */
    @JsonIgnore
    Put getPut(int salt) throws Exception {
        Put put = new Put(getKey(salt));

        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            Field field = entry.getValue().getDecoratedField();
            byte[] value = ToBytes(field.get(this), field.getType());
            if (value == null) {
                throw new Exception("Unsupported field data type");
            }
            put.addColumn(entry.getValue().getBinFamily(), entry.getValue().getBinName(), value);
        }

        onAfterPut(put, hBaseColumnInfoMap);

        return put;
    }

    /**
     * Generates Get from the fields decorated with {@link org.gooseman.hbase.HBaseColumn} class.
     * @param salt
     * @return
     */
    @JsonIgnore
    Get getGet(int salt) {
        Get get = new Get(getKey(salt));

        hBaseColumnInfoMap.forEach((s, hBaseColumnInfo) -> get.addColumn(hBaseColumnInfo.getBinFamily(),
                hBaseColumnInfo.getBinName()));

        onAfterGet(get, hBaseColumnInfoMap);

        return get;
    }

    /**
     * Update fields decorated with {@link org.gooseman.hbase.HBaseColumn} with values from HBase {@link Result}
     * @param result
     */
    void updateFieldsFromResult(Result result) throws IllegalAccessException {
        for(Map.Entry<String, HBaseColumnInfo> entry : hBaseColumnInfoMap.entrySet()) {
            HBaseColumnInfo ci = entry.getValue();
            if (result.containsColumn(ci.getBinFamily(), ci.getBinName())) {
                Field field = ci.getDecoratedField();
                byte[] value = result.getValue(ci.getBinFamily(), ci.getBinName());
                field.set(this, FromBytes(value, field.getType()));
            }
        }
    }
}
