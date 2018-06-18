package com.kb.hbase.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

public class RegionCompactor {

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
                    String strTableName = tableName.getNameAsString();
                    List<HRegionInfo> regions = hbaseAdmin.getTableRegions(tableName);
                    if (regions != null) {
                        int noOfRegions = regions.size();
                        int processedRegionsCount = 1;
                        for (HRegionInfo hRegionInfo : regions) {
                            byte[] region = hRegionInfo.getRegionName();
                            try {
                                hbaseAdmin.majorCompactRegion(region);
                                CompactionState compactionState = hbaseAdmin.getCompactionStateForRegion(region);
                                do {
                                    Thread.sleep(10000l);
                                    compactionState = hbaseAdmin.getCompactionStateForRegion(region);
                                } while (compactionState != CompactionState.NONE);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            processedRegionsCount++;
                        }
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
