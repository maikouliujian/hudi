/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * All the metadata that gets stored along with a commit.
 */
//todo 将timeline上.commit文件内容转化为HoodieCommitMetadata

/***
 * {
 *   "partitionToWriteStats" : {
 *     "2023-04-22/th" : [ {
 *       "fileId" : "5df79bf8-4c6d-4491-ad7c-d73468016220-0",
 *       "path" : "2023-04-22/th/5df79bf8-4c6d-4491-ad7c-d73468016220-0_0-40-5833_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 1580,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 172,
 *       "numInserts" : 170,
 *       "totalWriteBytes" : 522464,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/th",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 522464,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/sg" : [ {
 *       "fileId" : "5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0",
 *       "path" : "2023-04-22/sg/5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0_1-40-5834_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 632,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 53,
 *       "numInserts" : 66,
 *       "totalWriteBytes" : 472331,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/sg",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 472331,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/jp" : [ {
 *       "fileId" : "88efb730-be99-430e-8f46-8ef89a9f3e56-0",
 *       "path" : "2023-04-22/jp/88efb730-be99-430e-8f46-8ef89a9f3e56-0_2-40-5835_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 10068,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 938,
 *       "numInserts" : 978,
 *       "totalWriteBytes" : 1056204,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/jp",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 1056204,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/kr" : [ {
 *       "fileId" : "fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0",
 *       "path" : "2023-04-22/kr/fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0_3-40-5836_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 2651,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 197,
 *       "numInserts" : 239,
 *       "totalWriteBytes" : 591865,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/kr",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 591865,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/ph" : [ {
 *       "fileId" : "5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0",
 *       "path" : "2023-04-22/ph/5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0_4-40-5837_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 4086,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 516,
 *       "numInserts" : 343,
 *       "totalWriteBytes" : 652932,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/ph",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 652932,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/my" : [ {
 *       "fileId" : "6fd335f6-f111-45ca-a156-36956ff3890b-0",
 *       "path" : "2023-04-22/my/6fd335f6-f111-45ca-a156-36956ff3890b-0_5-40-5838_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 1284,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 108,
 *       "numInserts" : 125,
 *       "totalWriteBytes" : 506558,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/my",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 506558,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-22/tw" : [ {
 *       "fileId" : "53e4e822-9e15-420a-bf78-88b3178820aa-0",
 *       "path" : "2023-04-22/tw/53e4e822-9e15-420a-bf78-88b3178820aa-0_6-40-5839_20230422040921737.parquet",
 *       "prevCommit" : "20230422035944132",
 *       "numWrites" : 724,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 71,
 *       "numInserts" : 66,
 *       "totalWriteBytes" : 477452,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-22/tw",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 477452,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ]
 *   },
 *   "compacted" : false,
 *   "extraMetadata" : {
 *     "schema" : "{\"type\":\"record\",\"name\":\"dim_user_active_1d_hudi_flink_record\",\"namespace\":\"hoodie.dim_user_active_1d_hudi_flink\",\"fields\":[{\"name\":\"user_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"download_channel\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"activation_day\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_timezone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uq_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"ip\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"pkg\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_day\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_region\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
 *   },
 *   "operationType" : "UPSERT",
 *   "writeStats" : [ {
 *     "fileId" : "5df79bf8-4c6d-4491-ad7c-d73468016220-0",
 *     "path" : "2023-04-22/th/5df79bf8-4c6d-4491-ad7c-d73468016220-0_0-40-5833_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 1580,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 172,
 *     "numInserts" : 170,
 *     "totalWriteBytes" : 522464,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/th",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 522464,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0",
 *     "path" : "2023-04-22/sg/5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0_1-40-5834_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 632,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 53,
 *     "numInserts" : 66,
 *     "totalWriteBytes" : 472331,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/sg",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 472331,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "88efb730-be99-430e-8f46-8ef89a9f3e56-0",
 *     "path" : "2023-04-22/jp/88efb730-be99-430e-8f46-8ef89a9f3e56-0_2-40-5835_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 10068,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 938,
 *     "numInserts" : 978,
 *     "totalWriteBytes" : 1056204,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/jp",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 1056204,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0",
 *     "path" : "2023-04-22/kr/fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0_3-40-5836_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 2651,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 197,
 *     "numInserts" : 239,
 *     "totalWriteBytes" : 591865,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/kr",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 591865,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0",
 *     "path" : "2023-04-22/ph/5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0_4-40-5837_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 4086,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 516,
 *     "numInserts" : 343,
 *     "totalWriteBytes" : 652932,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/ph",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 652932,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "6fd335f6-f111-45ca-a156-36956ff3890b-0",
 *     "path" : "2023-04-22/my/6fd335f6-f111-45ca-a156-36956ff3890b-0_5-40-5838_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 1284,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 108,
 *     "numInserts" : 125,
 *     "totalWriteBytes" : 506558,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/my",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 506558,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "53e4e822-9e15-420a-bf78-88b3178820aa-0",
 *     "path" : "2023-04-22/tw/53e4e822-9e15-420a-bf78-88b3178820aa-0_6-40-5839_20230422040921737.parquet",
 *     "prevCommit" : "20230422035944132",
 *     "numWrites" : 724,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 71,
 *     "numInserts" : 66,
 *     "totalWriteBytes" : 477452,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-22/tw",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 477452,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   } ],
 *   "totalRecordsDeleted" : 0,
 *   "totalLogRecordsCompacted" : 0,
 *   "totalLogFilesCompacted" : 0,
 *   "totalCompactedRecordsUpdated" : 0,
 *   "totalLogFilesSize" : 0,
 *   "totalScanTime" : 0,
 *   "totalCreateTime" : 0,
 *   "minAndMaxEventTime" : {
 *     "Optional.empty" : {
 *       "val" : null,
 *       "present" : false
 *     }
 *   },
 *   "totalUpsertTime" : 8768,
 *   "writePartitionPaths" : [ "2023-04-22/th", "2023-04-22/sg", "2023-04-22/jp", "2023-04-22/kr", "2023-04-22/ph", "2023-04-22/my", "2023-04-22/tw" ],
 *   "fileIdAndRelativePaths" : {
 *     "6fd335f6-f111-45ca-a156-36956ff3890b-0" : "2023-04-22/my/6fd335f6-f111-45ca-a156-36956ff3890b-0_5-40-5838_20230422040921737.parquet",
 *     "88efb730-be99-430e-8f46-8ef89a9f3e56-0" : "2023-04-22/jp/88efb730-be99-430e-8f46-8ef89a9f3e56-0_2-40-5835_20230422040921737.parquet",
 *     "5df79bf8-4c6d-4491-ad7c-d73468016220-0" : "2023-04-22/th/5df79bf8-4c6d-4491-ad7c-d73468016220-0_0-40-5833_20230422040921737.parquet",
 *     "5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0" : "2023-04-22/sg/5fa0b516-04e0-4c52-b2c9-26bbcae6ede7-0_1-40-5834_20230422040921737.parquet",
 *     "fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0" : "2023-04-22/kr/fe156dcc-de43-4be2-a5e9-a4f4e534c92b-0_3-40-5836_20230422040921737.parquet",
 *     "5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0" : "2023-04-22/ph/5ce6fa08-c481-4d31-a4b1-4620d831d9eb-0_4-40-5837_20230422040921737.parquet",
 *     "53e4e822-9e15-420a-bf78-88b3178820aa-0" : "2023-04-22/tw/53e4e822-9e15-420a-bf78-88b3178820aa-0_6-40-5839_20230422040921737.parquet"
 *   }
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieCommitMetadata implements Serializable {

  public static final String SCHEMA_KEY = "schema";
  private static final Logger LOG = LogManager.getLogger(HoodieCommitMetadata.class);
  //todo partitionToWriteStats <partitionpath,List<HoodieWriteStat>>
  protected Map<String, List<HoodieWriteStat>> partitionToWriteStats;
  protected Boolean compacted;

  protected Map<String, String> extraMetadata;
  //todo operationType【不同的操作类型】
  protected WriteOperationType operationType = WriteOperationType.UNKNOWN;

  // for ser/deser
  public HoodieCommitMetadata() {
    this(false);
  }

  public HoodieCommitMetadata(boolean compacted) {
    extraMetadata = new HashMap<>();
    partitionToWriteStats = new HashMap<>();
    this.compacted = compacted;
  }

  public void addWriteStat(String partitionPath, HoodieWriteStat stat) {
    if (!partitionToWriteStats.containsKey(partitionPath)) {
      partitionToWriteStats.put(partitionPath, new ArrayList<>());
    }
    partitionToWriteStats.get(partitionPath).add(stat);
  }

  public void addMetadata(String metaKey, String value) {
    extraMetadata.put(metaKey, value);
  }

  public List<HoodieWriteStat> getWriteStats(String partitionPath) {
    return partitionToWriteStats.get(partitionPath);
  }

  public Map<String, String> getExtraMetadata() {
    return extraMetadata;
  }

  public Map<String, List<HoodieWriteStat>> getPartitionToWriteStats() {
    return partitionToWriteStats;
  }

  public List<HoodieWriteStat> getWriteStats() {
    return partitionToWriteStats.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  public String getMetadata(String metaKey) {
    return extraMetadata.get(metaKey);
  }

  public Boolean getCompacted() {
    return compacted;
  }

  public void setCompacted(Boolean compacted) {
    this.compacted = compacted;
  }

  public HashMap<String, String> getFileIdAndRelativePaths() {
    HashMap<String, String> filePaths = new HashMap<>();
    // list all partitions paths
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      for (HoodieWriteStat stat : stats) {
        filePaths.put(stat.getFileId(), stat.getPath());
      }
    }
    return filePaths;
  }

  public void setOperationType(WriteOperationType type) {
    this.operationType = type;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  public HashMap<String, String> getFileIdAndFullPaths(Path basePath) {
    HashMap<String, String> fullPaths = new HashMap<>();
    for (Map.Entry<String, String> entry : getFileIdAndRelativePaths().entrySet()) {
      String fullPath = entry.getValue() != null
          ? FSUtils.getPartitionPath(basePath, entry.getValue()).toString()
          : null;
      fullPaths.put(entry.getKey(), fullPath);
    }
    return fullPaths;
  }

  public List<String> getFullPathsByPartitionPath(String basePath, String partitionPath) {
    HashSet<String> fullPaths = new HashSet<>();
    if (getPartitionToWriteStats().get(partitionPath) != null) {
      for (HoodieWriteStat stat : getPartitionToWriteStats().get(partitionPath)) {
        if ((stat.getFileId() != null)) {
          String fullPath = FSUtils.getPartitionPath(basePath, stat.getPath()).toString();
          fullPaths.add(fullPath);
        }
      }
    }
    return new ArrayList<>(fullPaths);
  }

  public Map<HoodieFileGroupId, String> getFileGroupIdAndFullPaths(String basePath) {
    Map<HoodieFileGroupId, String> fileGroupIdToFullPaths = new HashMap<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId(stat.getPartitionPath(), stat.getFileId());
        Path fullPath = new Path(basePath, stat.getPath());
        fileGroupIdToFullPaths.put(fileGroupId, fullPath.toString());
      }
    }
    return fileGroupIdToFullPaths;
  }

  /**
   * Extract the file status of all affected files from the commit metadata. If a file has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit.
   *
   *
   * @param hadoopConf
   * @param basePath The base path
   * @return the file full path to file status mapping
   */
  public Map<String, FileStatus> getFullPathToFileStatus(Configuration hadoopConf, String basePath) {
    Map<String, FileStatus> fullPathToFileStatus = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        Path fullPath = relativeFilePath != null ? FSUtils.getPartitionPath(basePath, relativeFilePath) : null;
        if (fullPath != null) {
          long blockSize = FSUtils.getFs(fullPath.toString(), hadoopConf).getDefaultBlockSize(fullPath);
          FileStatus fileStatus = new FileStatus(stat.getFileSizeInBytes(), false, 0, blockSize,
              0, fullPath);
          fullPathToFileStatus.put(fullPath.getName(), fileStatus);
        }
      }
    }
    return fullPathToFileStatus;
  }

  /**
   * Extract the file status of all affected files from the commit metadata. If a file has
   * been touched multiple times in the given commits, the return value will keep the one
   * from the latest commit by file group ID.
   *
   * <p>Note: different with {@link #getFullPathToFileStatus(Configuration, String)},
   * only the latest commit file for a file group is returned,
   * this is an optimization for COPY_ON_WRITE table to eliminate legacy files for filesystem view.
   *
   *
   * @param hadoopConf
   * @param basePath The base path
   * @return the file ID to file status mapping
   */
  public Map<String, FileStatus> getFileIdToFileStatus(Configuration hadoopConf, String basePath) {
    Map<String, FileStatus> fileIdToFileStatus = new HashMap<>();
    for (List<HoodieWriteStat> stats : getPartitionToWriteStats().values()) {
      // Iterate through all the written files.
      for (HoodieWriteStat stat : stats) {
        String relativeFilePath = stat.getPath();
        Path fullPath = relativeFilePath != null ? FSUtils.getPartitionPath(basePath, relativeFilePath) : null;
        if (fullPath != null) {
          FileStatus fileStatus = new FileStatus(stat.getFileSizeInBytes(), false, 0, 0,
              0, fullPath);
          fileIdToFileStatus.put(stat.getFileId(), fileStatus);
        }
      }
    }
    return fileIdToFileStatus;
  }

  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToWriteStats.get(null));
      partitionToWriteStats.remove(null);
    }
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or somethings bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  /**
   * parse the bytes of deltacommit, and get the base file and the log files belonging to this
   * provided file group.
   */
  // TODO: refactor this method to avoid doing the json tree walking (HUDI-4822).
  public static Option<Pair<String, List<String>>> getFileSliceForFileGroupFromDeltaCommit(
      byte[] bytes, HoodieFileGroupId fileGroupId) {
    String jsonStr = new String(bytes, StandardCharsets.UTF_8);
    if (jsonStr.isEmpty()) {
      return Option.empty();
    }

    try {
      JsonNode ptToWriteStatsMap = JsonUtils.getObjectMapper().readTree(jsonStr).get("partitionToWriteStats");
      Iterator<Map.Entry<String, JsonNode>> pts = ptToWriteStatsMap.fields();
      while (pts.hasNext()) {
        Map.Entry<String, JsonNode> ptToWriteStats = pts.next();
        if (ptToWriteStats.getValue().isArray()) {
          for (JsonNode writeStat : ptToWriteStats.getValue()) {
            HoodieFileGroupId fgId = new HoodieFileGroupId(ptToWriteStats.getKey(), writeStat.get("fileId").asText());
            if (fgId.equals(fileGroupId)) {
              String baseFile = writeStat.get("baseFile").asText();
              ArrayNode logFilesNode = (ArrayNode) writeStat.get("logFiles");
              List<String> logFiles = new ArrayList<>();
              for (JsonNode logFile : logFilesNode) {
                logFiles.add(logFile.asText());
              }
              return Option.of(Pair.of(baseFile, logFiles));
            }
          }
        }
      }
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieException("Fail to parse the base file and log files from DeltaCommit", e);
    }
  }

  // Here the functions are named "fetch" instead of "get", to get avoid of the json conversion.
  public long fetchTotalPartitionsWritten() {
    return partitionToWriteStats.size();
  }

  public long fetchTotalFilesInsert() {
    long totalFilesInsert = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit() != null && stat.getPrevCommit().equalsIgnoreCase("null")) {
          totalFilesInsert++;
        }
      }
    }
    return totalFilesInsert;
  }

  public long fetchTotalFilesUpdated() {
    long totalFilesUpdated = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit() != null && !stat.getPrevCommit().equalsIgnoreCase("null")) {
          totalFilesUpdated++;
        }
      }
    }
    return totalFilesUpdated;
  }

  public long fetchTotalUpdateRecordsWritten() {
    long totalUpdateRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalUpdateRecordsWritten += stat.getNumUpdateWrites();
      }
    }
    return totalUpdateRecordsWritten;
  }

  public long fetchTotalInsertRecordsWritten() {
    long totalInsertRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        // determine insert rows in every file
        if (stat.getPrevCommit() != null) {
          totalInsertRecordsWritten += stat.getNumInserts();
        }
      }
    }
    return totalInsertRecordsWritten;
  }

  public long fetchTotalRecordsWritten() {
    long totalRecordsWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalRecordsWritten += stat.getNumWrites();
      }
    }
    return totalRecordsWritten;
  }

  public long fetchTotalBytesWritten() {
    long totalBytesWritten = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalBytesWritten += stat.getTotalWriteBytes();
      }
    }
    return totalBytesWritten;
  }

  public long fetchTotalWriteErrors() {
    long totalWriteErrors = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalWriteErrors += stat.getTotalWriteErrors();
      }
    }
    return totalWriteErrors;
  }

  public long getTotalRecordsDeleted() {
    long totalDeletes = 0;
    for (List<HoodieWriteStat> stats : partitionToWriteStats.values()) {
      for (HoodieWriteStat stat : stats) {
        totalDeletes += stat.getNumDeletes();
      }
    }
    return totalDeletes;
  }

  public Long getTotalLogRecordsCompacted() {
    Long totalLogRecords = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogRecords += writeStat.getTotalLogRecords();
      }
    }
    return totalLogRecords;
  }

  public Long getTotalLogFilesCompacted() {
    Long totalLogFiles = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogFiles += writeStat.getTotalLogFilesCompacted();
      }
    }
    return totalLogFiles;
  }

  public Long getTotalCompactedRecordsUpdated() {
    Long totalUpdateRecords = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalUpdateRecords += writeStat.getTotalUpdatedRecordsCompacted();
      }
    }
    return totalUpdateRecords;
  }

  public Long getTotalLogFilesSize() {
    Long totalLogFilesSize = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        totalLogFilesSize += writeStat.getTotalLogSizeCompacted();
      }
    }
    return totalLogFilesSize;
  }

  public Long getTotalScanTime() {
    Long totalScanTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalScanTime += writeStat.getRuntimeStats().getTotalScanTime();
        }
      }
    }
    return totalScanTime;
  }

  public Long getTotalCreateTime() {
    Long totalCreateTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalCreateTime += writeStat.getRuntimeStats().getTotalCreateTime();
        }
      }
    }
    return totalCreateTime;
  }

  public Long getTotalUpsertTime() {
    Long totalUpsertTime = 0L;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        if (writeStat.getRuntimeStats() != null) {
          totalUpsertTime += writeStat.getRuntimeStats().getTotalUpsertTime();
        }
      }
    }
    return totalUpsertTime;
  }

  public Pair<Option<Long>, Option<Long>> getMinAndMaxEventTime() {
    long minEventTime = Long.MAX_VALUE;
    long maxEventTime = Long.MIN_VALUE;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : partitionToWriteStats.entrySet()) {
      for (HoodieWriteStat writeStat : entry.getValue()) {
        minEventTime = writeStat.getMinEventTime() != null ? Math.min(writeStat.getMinEventTime(), minEventTime) : minEventTime;
        maxEventTime = writeStat.getMaxEventTime() != null ? Math.max(writeStat.getMaxEventTime(), maxEventTime) : maxEventTime;
      }
    }
    return Pair.of(
        minEventTime == Long.MAX_VALUE ? Option.empty() : Option.of(minEventTime),
        maxEventTime == Long.MIN_VALUE ? Option.empty() : Option.of(maxEventTime));
  }

  public HashSet<String> getWritePartitionPaths() {
    return new HashSet<>(partitionToWriteStats.keySet());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieCommitMetadata that = (HoodieCommitMetadata) o;

    if (!partitionToWriteStats.equals(that.partitionToWriteStats)) {
      return false;
    }
    return compacted.equals(that.compacted);

  }

  @Override
  public int hashCode() {
    int result = partitionToWriteStats.hashCode();
    result = 31 * result + compacted.hashCode();
    return result;
  }

  public static <T> T fromBytes(byte[] bytes, Class<T> clazz) throws IOException {
    try {
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), clazz);
    } catch (Exception e) {
      throw new IOException("unable to read commit metadata", e);
    }
  }

  @Override
  public String toString() {
    return "HoodieCommitMetadata{" + "partitionToWriteStats=" + partitionToWriteStats
        + ", compacted=" + compacted
        + ", extraMetadata=" + extraMetadata
        + ", operationType=" + operationType + '}';
  }
}
