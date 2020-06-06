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
import org.apache.hadoop.hbase.util.Bytes;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class HBaseInfo {

    private final TableName tableName;
    private final byte salt;
    private final Stream<byte[]> saltStream;

    public HBaseInfo(HBase hBase) {
        this.tableName = TableName.valueOf(hBase.tableName());
        this.salt = hBase.salt();

        saltStream = IntStream.range(0, this.salt).boxed().map(i -> Bytes.toBytes(String.valueOf((char)i.byteValue())));
    }

    public TableName getTableName() {
        return this.tableName;
    }

    public byte getSalt() {
        return salt;
    }

    public Stream<byte[]> getSaltStream() {
        return saltStream;
    }
}
