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

import static org.apache.hadoop.hbase.util.Bytes.*;

import java.lang.reflect.Field;

public class HBaseColumnInfo {

    private byte[] binName;
    private byte[] binFamily;
    private Field decoratedField;

    public HBaseColumnInfo(HBaseColumn hBaseColumn, Field field) {
        binName = toBytes(hBaseColumn.name().isEmpty() ? field.getName() : hBaseColumn.name());
        binFamily = toBytes(hBaseColumn.family());
        decoratedField = field;
    }

    public byte[] getBinName() {
        return binName;
    }

    public byte[] getBinFamily() {
        return binFamily;
    }

    public Field getDecoratedField() {
        return decoratedField;
    }
}