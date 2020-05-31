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

import org.apache.hadoop.hbase.TableName;

public class HBaseInfo {

    private final TableName tableName;
    private final int salt;

    public HBaseInfo(HBase hBase) {
        this.tableName = TableName.valueOf(hBase.tableName());
        this.salt = hBase.salt();
    }

    public TableName getTableName() {
        return this.tableName;
    }

    public int getSalt() {
        return salt;
    }
}
