package com.kb.hbase.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

public class TableCompactor {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing zookeeper hostname");
        }
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", args[0]);
        HBaseAdmin hbaseAdmin = null;
        try {
            hbaseAdmin = new HBaseAdmin(config);
            TableName[] tableNames = hbaseAdmin.listTableNames();
            if (tableNames != null) {
                int noOfTables = tableNames.length;
                int processedTablesCount = 1;
                for (TableName tableName : tableNames) {

                    try {
                        hbaseAdmin.majorCompact(tableName);
                        CompactionState compactionState = hbaseAdmin.getCompactionState(tableName);
                        do {
                            Thread.sleep(10000l);
                            compactionState = hbaseAdmin.getCompactionState(tableName);

                        } while (compactionState != CompactionState.NONE);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (hbaseAdmin != null) {
                try {
                    hbaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
