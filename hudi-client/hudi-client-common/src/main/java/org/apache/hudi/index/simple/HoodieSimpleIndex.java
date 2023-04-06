/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.index.simple;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * A simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 */
public class HoodieSimpleIndex
    extends HoodieIndex<Object, Object> {

  private final Option<BaseKeyGenerator> keyGeneratorOpt;

  public HoodieSimpleIndex(HoodieWriteConfig config, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config);
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return tagLocationInternal(records, context, hoodieTable);
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecords {@link HoodieData} of incoming records
   * @param context      instance of {@link HoodieEngineContext} to use
   * @param hoodieTable  instance of {@link HoodieTable} to use
   * @return {@link HoodieData} of records with record locations set
   */
  protected <R> HoodieData<HoodieRecord<R>> tagLocationInternal(
      HoodieData<HoodieRecord<R>> inputRecords, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    if (config.getSimpleIndexUseCaching()) {
      inputRecords.persist(new HoodieConfig(config.getProps())
          .getString(HoodieIndexConfig.SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE));
    }

    HoodiePairData<HoodieKey, HoodieRecord<R>> keyedInputRecords =
        inputRecords.mapToPair(record -> new ImmutablePair<>(record.getKey(), record));
    //todo 找到inputRecords将要写入的多个partition path对应的last basefile中已存在的每条记录的<HoodieKey, HoodieRecordLocation>映射关系
    HoodiePairData<HoodieKey, HoodieRecordLocation> existingLocationsOnTable =
        fetchRecordLocationsForAffectedPartitions(keyedInputRecords.keys(), context, hoodieTable,
            config.getSimpleIndexParallelism());

    HoodieData<HoodieRecord<R>> taggedRecords =
        keyedInputRecords.leftOuterJoin(existingLocationsOnTable).map(entry -> {
          //todo 待写入的数据
          final HoodieRecord<R> untaggedRecord = entry.getRight().getLeft();
          //todo 如果是insert，则为null；如果是update，则是已存在数据的HoodieRecordLocation
          final Option<HoodieRecordLocation> location = Option.ofNullable(entry.getRight().getRight().orElse(null));
          return HoodieIndexUtils.getTaggedRecord(untaggedRecord, location);
        });

    if (config.getSimpleIndexUseCaching()) {
      inputRecords.unpersist();
    }
    return taggedRecords;
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param hoodieKeys  {@link HoodieData} of {@link HoodieKey}s for which locations are fetched
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link HoodiePairData} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  protected HoodiePairData<HoodieKey, HoodieRecordLocation> fetchRecordLocationsForAffectedPartitions(
      HoodieData<HoodieKey> hoodieKeys, HoodieEngineContext context, HoodieTable hoodieTable,
      int parallelism) {
    //todo 要处理数据hoodieKeys的分区【这个分区一般都是按照时间最新的】
    List<String> affectedPartitionPathList =
        hoodieKeys.map(HoodieKey::getPartitionPath).distinct().collectAsList();
    List<Pair<String, HoodieBaseFile>> latestBaseFiles =
            //todo 找到每个partition path对应的最近的basefile
        getLatestBaseFilesForAllPartitions(affectedPartitionPathList, context, hoodieTable);
    return fetchRecordLocations(context, hoodieTable, parallelism, latestBaseFiles);
  }

  //todo 获取所有baseFiles中所有记录的HoodieRecordLocation
  protected HoodiePairData<HoodieKey, HoodieRecordLocation> fetchRecordLocations(
      HoodieEngineContext context, HoodieTable hoodieTable, int parallelism,
      //todo baseFiles ===> <partitionpath,lastbasefile>
      List<Pair<String, HoodieBaseFile>> baseFiles) {
    int fetchParallelism = Math.max(1, Math.min(baseFiles.size(), parallelism));

    return context.parallelize(baseFiles, fetchParallelism)
        .flatMap(partitionPathBaseFile -> new HoodieKeyLocationFetchHandle(config, hoodieTable, partitionPathBaseFile, keyGeneratorOpt)
            .locations().iterator())
        .mapToPair(e -> (Pair<HoodieKey, HoodieRecordLocation>) e);
  }
}
