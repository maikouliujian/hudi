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

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All the metadata that gets stored along with a commit.
 */

/***
 * {
 *   "partitionToWriteStats" : {
 *     "2023-04-24/04/my/appsdk" : [ {
 *       "fileId" : "6302658c-a758-4ee0-bfb4-8f427c1774cc-0",
 *       "path" : "2023-04-24/04/my/appsdk/6302658c-a758-4ee0-bfb4-8f427c1774cc-0_0-16-3429_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 67930,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 67930,
 *       "totalWriteBytes" : 9438462,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/my/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 9438462,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/jp/appsdk" : [ {
 *       "fileId" : "75399fc4-7ae5-4613-86d3-26e82a4b81c1-0",
 *       "path" : "2023-04-24/04/jp/appsdk/75399fc4-7ae5-4613-86d3-26e82a4b81c1-0_1-16-3430_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 569110,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 569110,
 *       "totalWriteBytes" : 81512883,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/jp/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 81512883,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/sg/server" : [ {
 *       "fileId" : "8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0",
 *       "path" : "2023-04-24/04/sg/server/8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0_2-16-3431_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 1629,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 1629,
 *       "totalWriteBytes" : 547980,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/sg/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 547980,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/global/appsdk" : [ {
 *       "fileId" : "5e23170f-5930-4f39-8927-d39279ffbd1b-0",
 *       "path" : "2023-04-24/04/global/appsdk/5e23170f-5930-4f39-8927-d39279ffbd1b-0_3-16-3432_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 17252,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 17252,
 *       "totalWriteBytes" : 2796453,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/global/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 2796453,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/tw/server" : [ {
 *       "fileId" : "61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0",
 *       "path" : "2023-04-24/04/tw/server/61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0_4-16-3433_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 1769,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 1769,
 *       "totalWriteBytes" : 556684,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/tw/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 556684,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/ph/appsdk" : [ {
 *       "fileId" : "da19e591-256c-45a8-835b-b8ca25d0ea19-0",
 *       "path" : "2023-04-24/04/ph/appsdk/da19e591-256c-45a8-835b-b8ca25d0ea19-0_5-16-3434_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 438905,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 438905,
 *       "totalWriteBytes" : 61296409,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/ph/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 61296409,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/tw/appsdk" : [ {
 *       "fileId" : "1fc48462-8c00-491c-bc0a-11e1667f6d96-0",
 *       "path" : "2023-04-24/04/tw/appsdk/1fc48462-8c00-491c-bc0a-11e1667f6d96-0_6-16-3435_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 62702,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 62702,
 *       "totalWriteBytes" : 9455815,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/tw/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 9455815,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/my/server" : [ {
 *       "fileId" : "7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0",
 *       "path" : "2023-04-24/04/my/server/7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0_7-16-3436_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 4778,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 4778,
 *       "totalWriteBytes" : 724848,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/my/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 724848,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/th/server" : [ {
 *       "fileId" : "e17a8a26-aa36-4442-9482-92f42a6dd601-0",
 *       "path" : "2023-04-24/04/th/server/e17a8a26-aa36-4442-9482-92f42a6dd601-0_8-16-3437_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 6491,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 6491,
 *       "totalWriteBytes" : 845144,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/th/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 845144,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/sg/appsdk" : [ {
 *       "fileId" : "ae575c08-b13c-491d-a543-74b7b3f7165a-0",
 *       "path" : "2023-04-24/04/sg/appsdk/ae575c08-b13c-491d-a543-74b7b3f7165a-0_9-16-3438_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 41344,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 41344,
 *       "totalWriteBytes" : 5965763,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/sg/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 5965763,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/global/server" : [ {
 *       "fileId" : "614ce71b-7400-47b3-a29c-1df47fec10b6-0",
 *       "path" : "2023-04-24/04/global/server/614ce71b-7400-47b3-a29c-1df47fec10b6-0_10-16-3439_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 1200,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 1200,
 *       "totalWriteBytes" : 526101,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/global/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 526101,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/th/appsdk" : [ {
 *       "fileId" : "6ca3b011-2643-4e1c-9372-d68bc0200c02-0",
 *       "path" : "2023-04-24/04/th/appsdk/6ca3b011-2643-4e1c-9372-d68bc0200c02-0_11-16-3440_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 191447,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 191447,
 *       "totalWriteBytes" : 23578906,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/th/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 23578906,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/kr/appsdk" : [ {
 *       "fileId" : "f2e498b6-9da6-49b3-944a-3c3fec970e4c-0",
 *       "path" : "2023-04-24/04/kr/appsdk/f2e498b6-9da6-49b3-944a-3c3fec970e4c-0_12-16-3441_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 125980,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 125980,
 *       "totalWriteBytes" : 17164532,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/kr/appsdk",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 17164532,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/ph/server" : [ {
 *       "fileId" : "b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0",
 *       "path" : "2023-04-24/04/ph/server/b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0_13-16-3442_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 28000,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 28000,
 *       "totalWriteBytes" : 2138685,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/ph/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 2138685,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/jp/server" : [ {
 *       "fileId" : "1aea8498-d393-4d94-be70-27f4bdc36d26-0",
 *       "path" : "2023-04-24/04/jp/server/1aea8498-d393-4d94-be70-27f4bdc36d26-0_14-16-3443_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 24618,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 24618,
 *       "totalWriteBytes" : 2055404,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/jp/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 2055404,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ],
 *     "2023-04-24/04/kr/server" : [ {
 *       "fileId" : "bbb2053e-6385-43d9-bc40-05d9077a7bb6-0",
 *       "path" : "2023-04-24/04/kr/server/bbb2053e-6385-43d9-bc40-05d9077a7bb6-0_15-16-3444_20230424073816368.parquet",
 *       "prevCommit" : "null",
 *       "numWrites" : 5932,
 *       "numDeletes" : 0,
 *       "numUpdateWrites" : 0,
 *       "numInserts" : 5932,
 *       "totalWriteBytes" : 814545,
 *       "totalWriteErrors" : 0,
 *       "tempPath" : null,
 *       "partitionPath" : "2023-04-24/04/kr/server",
 *       "totalLogRecords" : 0,
 *       "totalLogFilesCompacted" : 0,
 *       "totalLogSizeCompacted" : 0,
 *       "totalUpdatedRecordsCompacted" : 0,
 *       "totalLogBlocks" : 0,
 *       "totalCorruptLogBlock" : 0,
 *       "totalRollbackBlocks" : 0,
 *       "fileSizeInBytes" : 814545,
 *       "minEventTime" : null,
 *       "maxEventTime" : null
 *     } ]
 *   },
 *   "compacted" : false,
 *   "extraMetadata" : {
 *     "schema" : "{\"type\":\"record\",\"name\":\"dwd_event_user_general_hudi_flink_record\",\"namespace\":\"hoodie.dwd_event_user_general_hudi_flink\",\"fields\":[{\"name\":\"event\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"carrier\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"wifi\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"is_login_id\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"uq_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"user_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"platform\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"brand\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"os_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"screen_height\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"screen_width\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"model\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"manufacturer\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"mcc\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"mnc\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"network_type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"_flush_time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"timezone_offset\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"process_time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"app_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"app_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"download_channel\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"_track_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"api_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"install_source\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"language\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"previouse_page\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"duration_time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"action_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"page\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"card\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_timezone\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"trans_info\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"string\",\"null\"]}],\"default\":null},{\"name\":\"context\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"string\",\"null\"]}],\"default\":null},{\"name\":\"previous_page\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"build_version\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"pkg\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"risk_info\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"string\",\"null\"]}],\"default\":null},{\"name\":\"receive_time\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"p_day\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_hour\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_region\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"p_type\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
 *   },
 *   "operationType" : "INSERT_OVERWRITE",
 *   "partitionToReplaceFileIds" : {
 *     "2023-04-24/04/sg/appsdk" : [ ],
 *     "2023-04-24/04/jp/appsdk" : [ ],
 *     "2023-04-24/04/kr/appsdk" : [ ],
 *     "2023-04-24/04/th/appsdk" : [ ],
 *     "2023-04-24/04/ph/server" : [ ],
 *     "2023-04-24/04/tw/server" : [ ],
 *     "2023-04-24/04/my/server" : [ ],
 *     "2023-04-24/04/sg/server" : [ ],
 *     "2023-04-24/04/global/server" : [ ],
 *     "2023-04-24/04/tw/appsdk" : [ ],
 *     "2023-04-24/04/my/appsdk" : [ ],
 *     "2023-04-24/04/jp/server" : [ ],
 *     "2023-04-24/04/ph/appsdk" : [ ],
 *     "2023-04-24/04/global/appsdk" : [ ],
 *     "2023-04-24/04/kr/server" : [ ],
 *     "2023-04-24/04/th/server" : [ ]
 *   },
 *   "writePartitionPaths" : [ "2023-04-24/04/my/appsdk", "2023-04-24/04/jp/appsdk", "2023-04-24/04/sg/server", "2023-04-24/04/global/appsdk", "2023-04-24/04/tw/server", "2023-04-24/04/ph/appsdk", "2023-04-24/04/tw/appsdk", "2023-04-24/04/my/server", "2023-04-24/04/th/server", "2023-04-24/04/sg/appsdk", "2023-04-24/04/global/server", "2023-04-24/04/th/appsdk", "2023-04-24/04/kr/appsdk", "2023-04-24/04/ph/server", "2023-04-24/04/jp/server", "2023-04-24/04/kr/server" ],
 *   "totalRecordsDeleted" : 0,
 *   "totalLogRecordsCompacted" : 0,
 *   "totalLogFilesCompacted" : 0,
 *   "totalCompactedRecordsUpdated" : 0,
 *   "totalLogFilesSize" : 0,
 *   "totalScanTime" : 0,
 *   "totalCreateTime" : 107346,
 *   "totalUpsertTime" : 0,
 *   "minAndMaxEventTime" : {
 *     "Optional.empty" : {
 *       "val" : null,
 *       "present" : false
 *     }
 *   },
 *   "fileIdAndRelativePaths" : {
 *     "bbb2053e-6385-43d9-bc40-05d9077a7bb6-0" : "2023-04-24/04/kr/server/bbb2053e-6385-43d9-bc40-05d9077a7bb6-0_15-16-3444_20230424073816368.parquet",
 *     "75399fc4-7ae5-4613-86d3-26e82a4b81c1-0" : "2023-04-24/04/jp/appsdk/75399fc4-7ae5-4613-86d3-26e82a4b81c1-0_1-16-3430_20230424073816368.parquet",
 *     "6302658c-a758-4ee0-bfb4-8f427c1774cc-0" : "2023-04-24/04/my/appsdk/6302658c-a758-4ee0-bfb4-8f427c1774cc-0_0-16-3429_20230424073816368.parquet",
 *     "da19e591-256c-45a8-835b-b8ca25d0ea19-0" : "2023-04-24/04/ph/appsdk/da19e591-256c-45a8-835b-b8ca25d0ea19-0_5-16-3434_20230424073816368.parquet",
 *     "5e23170f-5930-4f39-8927-d39279ffbd1b-0" : "2023-04-24/04/global/appsdk/5e23170f-5930-4f39-8927-d39279ffbd1b-0_3-16-3432_20230424073816368.parquet",
 *     "614ce71b-7400-47b3-a29c-1df47fec10b6-0" : "2023-04-24/04/global/server/614ce71b-7400-47b3-a29c-1df47fec10b6-0_10-16-3439_20230424073816368.parquet",
 *     "b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0" : "2023-04-24/04/ph/server/b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0_13-16-3442_20230424073816368.parquet",
 *     "61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0" : "2023-04-24/04/tw/server/61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0_4-16-3433_20230424073816368.parquet",
 *     "f2e498b6-9da6-49b3-944a-3c3fec970e4c-0" : "2023-04-24/04/kr/appsdk/f2e498b6-9da6-49b3-944a-3c3fec970e4c-0_12-16-3441_20230424073816368.parquet",
 *     "8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0" : "2023-04-24/04/sg/server/8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0_2-16-3431_20230424073816368.parquet",
 *     "7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0" : "2023-04-24/04/my/server/7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0_7-16-3436_20230424073816368.parquet",
 *     "6ca3b011-2643-4e1c-9372-d68bc0200c02-0" : "2023-04-24/04/th/appsdk/6ca3b011-2643-4e1c-9372-d68bc0200c02-0_11-16-3440_20230424073816368.parquet",
 *     "1aea8498-d393-4d94-be70-27f4bdc36d26-0" : "2023-04-24/04/jp/server/1aea8498-d393-4d94-be70-27f4bdc36d26-0_14-16-3443_20230424073816368.parquet",
 *     "ae575c08-b13c-491d-a543-74b7b3f7165a-0" : "2023-04-24/04/sg/appsdk/ae575c08-b13c-491d-a543-74b7b3f7165a-0_9-16-3438_20230424073816368.parquet",
 *     "1fc48462-8c00-491c-bc0a-11e1667f6d96-0" : "2023-04-24/04/tw/appsdk/1fc48462-8c00-491c-bc0a-11e1667f6d96-0_6-16-3435_20230424073816368.parquet",
 *     "e17a8a26-aa36-4442-9482-92f42a6dd601-0" : "2023-04-24/04/th/server/e17a8a26-aa36-4442-9482-92f42a6dd601-0_8-16-3437_20230424073816368.parquet"
 *   },
 *   "writeStats" : [ {
 *     "fileId" : "6302658c-a758-4ee0-bfb4-8f427c1774cc-0",
 *     "path" : "2023-04-24/04/my/appsdk/6302658c-a758-4ee0-bfb4-8f427c1774cc-0_0-16-3429_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 67930,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 67930,
 *     "totalWriteBytes" : 9438462,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/my/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 9438462,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "75399fc4-7ae5-4613-86d3-26e82a4b81c1-0",
 *     "path" : "2023-04-24/04/jp/appsdk/75399fc4-7ae5-4613-86d3-26e82a4b81c1-0_1-16-3430_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 569110,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 569110,
 *     "totalWriteBytes" : 81512883,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/jp/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 81512883,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0",
 *     "path" : "2023-04-24/04/sg/server/8dd6f921-5ff1-415d-9d98-138b0b3de0c3-0_2-16-3431_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 1629,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 1629,
 *     "totalWriteBytes" : 547980,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/sg/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 547980,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "5e23170f-5930-4f39-8927-d39279ffbd1b-0",
 *     "path" : "2023-04-24/04/global/appsdk/5e23170f-5930-4f39-8927-d39279ffbd1b-0_3-16-3432_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 17252,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 17252,
 *     "totalWriteBytes" : 2796453,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/global/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 2796453,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0",
 *     "path" : "2023-04-24/04/tw/server/61a6ee05-e44c-43b8-b04a-91d697e3a7e6-0_4-16-3433_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 1769,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 1769,
 *     "totalWriteBytes" : 556684,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/tw/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 556684,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "da19e591-256c-45a8-835b-b8ca25d0ea19-0",
 *     "path" : "2023-04-24/04/ph/appsdk/da19e591-256c-45a8-835b-b8ca25d0ea19-0_5-16-3434_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 438905,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 438905,
 *     "totalWriteBytes" : 61296409,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/ph/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 61296409,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "1fc48462-8c00-491c-bc0a-11e1667f6d96-0",
 *     "path" : "2023-04-24/04/tw/appsdk/1fc48462-8c00-491c-bc0a-11e1667f6d96-0_6-16-3435_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 62702,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 62702,
 *     "totalWriteBytes" : 9455815,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/tw/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 9455815,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0",
 *     "path" : "2023-04-24/04/my/server/7e41ba9e-2c08-4f8f-a7f9-b7b4930ad853-0_7-16-3436_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 4778,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 4778,
 *     "totalWriteBytes" : 724848,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/my/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 724848,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "e17a8a26-aa36-4442-9482-92f42a6dd601-0",
 *     "path" : "2023-04-24/04/th/server/e17a8a26-aa36-4442-9482-92f42a6dd601-0_8-16-3437_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 6491,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 6491,
 *     "totalWriteBytes" : 845144,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/th/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 845144,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "ae575c08-b13c-491d-a543-74b7b3f7165a-0",
 *     "path" : "2023-04-24/04/sg/appsdk/ae575c08-b13c-491d-a543-74b7b3f7165a-0_9-16-3438_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 41344,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 41344,
 *     "totalWriteBytes" : 5965763,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/sg/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 5965763,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "614ce71b-7400-47b3-a29c-1df47fec10b6-0",
 *     "path" : "2023-04-24/04/global/server/614ce71b-7400-47b3-a29c-1df47fec10b6-0_10-16-3439_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 1200,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 1200,
 *     "totalWriteBytes" : 526101,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/global/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 526101,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "6ca3b011-2643-4e1c-9372-d68bc0200c02-0",
 *     "path" : "2023-04-24/04/th/appsdk/6ca3b011-2643-4e1c-9372-d68bc0200c02-0_11-16-3440_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 191447,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 191447,
 *     "totalWriteBytes" : 23578906,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/th/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 23578906,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "f2e498b6-9da6-49b3-944a-3c3fec970e4c-0",
 *     "path" : "2023-04-24/04/kr/appsdk/f2e498b6-9da6-49b3-944a-3c3fec970e4c-0_12-16-3441_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 125980,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 125980,
 *     "totalWriteBytes" : 17164532,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/kr/appsdk",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 17164532,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0",
 *     "path" : "2023-04-24/04/ph/server/b5fa8d2c-0e68-4120-9baf-387d4027e7ac-0_13-16-3442_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 28000,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 28000,
 *     "totalWriteBytes" : 2138685,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/ph/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 2138685,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "1aea8498-d393-4d94-be70-27f4bdc36d26-0",
 *     "path" : "2023-04-24/04/jp/server/1aea8498-d393-4d94-be70-27f4bdc36d26-0_14-16-3443_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 24618,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 24618,
 *     "totalWriteBytes" : 2055404,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/jp/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 2055404,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   }, {
 *     "fileId" : "bbb2053e-6385-43d9-bc40-05d9077a7bb6-0",
 *     "path" : "2023-04-24/04/kr/server/bbb2053e-6385-43d9-bc40-05d9077a7bb6-0_15-16-3444_20230424073816368.parquet",
 *     "prevCommit" : "null",
 *     "numWrites" : 5932,
 *     "numDeletes" : 0,
 *     "numUpdateWrites" : 0,
 *     "numInserts" : 5932,
 *     "totalWriteBytes" : 814545,
 *     "totalWriteErrors" : 0,
 *     "tempPath" : null,
 *     "partitionPath" : "2023-04-24/04/kr/server",
 *     "totalLogRecords" : 0,
 *     "totalLogFilesCompacted" : 0,
 *     "totalLogSizeCompacted" : 0,
 *     "totalUpdatedRecordsCompacted" : 0,
 *     "totalLogBlocks" : 0,
 *     "totalCorruptLogBlock" : 0,
 *     "totalRollbackBlocks" : 0,
 *     "fileSizeInBytes" : 814545,
 *     "minEventTime" : null,
 *     "maxEventTime" : null
 *   } ]
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieReplaceCommitMetadata extends HoodieCommitMetadata {
  private static final Logger LOG = LogManager.getLogger(HoodieReplaceCommitMetadata.class);

  //todo <partitionpath,list<Replacefileid[要被替换的fileid,老文件id]>>
  //todo 此 replace commit要替换的fileid
  protected Map<String, List<String>> partitionToReplaceFileIds;

  // for ser/deser
  public HoodieReplaceCommitMetadata() {
    this(false);
  }

  public HoodieReplaceCommitMetadata(boolean compacted) {
    super(compacted);
    partitionToReplaceFileIds = new HashMap<>();
  }

  public void setPartitionToReplaceFileIds(Map<String, List<String>> partitionToReplaceFileIds) {
    this.partitionToReplaceFileIds = partitionToReplaceFileIds;
  }

  public void addReplaceFileId(String partitionPath, String fileId) {
    if (!partitionToReplaceFileIds.containsKey(partitionPath)) {
      partitionToReplaceFileIds.put(partitionPath, new ArrayList<>());
    }
    partitionToReplaceFileIds.get(partitionPath).add(fileId);
  }

  public List<String> getReplaceFileIds(String partitionPath) {
    return partitionToReplaceFileIds.get(partitionPath);
  }

  public Map<String, List<String>> getPartitionToReplaceFileIds() {
    return partitionToReplaceFileIds;
  }

  @Override
  public String toJsonString() throws IOException {
    if (partitionToWriteStats.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToWriteStats.get(null));
      partitionToWriteStats.remove(null);
    }
    if (partitionToReplaceFileIds.containsKey(null)) {
      LOG.info("partition path is null for " + partitionToReplaceFileIds.get(null));
      partitionToReplaceFileIds.remove(null);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieReplaceCommitMetadata that = (HoodieReplaceCommitMetadata) o;

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
  //todo 文件内容转化为javabean
  public static <T> T fromBytes(byte[] bytes, Class<T> clazz) throws IOException {
    try {
      return fromJsonString(new String(bytes, StandardCharsets.UTF_8), clazz);
    } catch (Exception e) {
      throw new IOException("unable to read commit metadata", e);
    }
  }

  @Override
  public String toString() {
    return "HoodieReplaceMetadata{" + "partitionToWriteStats=" + partitionToWriteStats
        + ", partitionToReplaceFileIds=" + partitionToReplaceFileIds
        + ", compacted=" + compacted
        + ", extraMetadata=" + extraMetadata
        + ", operationType=" + operationType + '}';
  }
}
