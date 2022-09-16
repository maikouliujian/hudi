/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.transform;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;

/**
 * Function that transforms RowData to HoodieRecord.
 */
public class RowDataToHoodieFunction<I extends RowData, O extends HoodieRecord>
    extends RichMapFunction<I, O> {
  /**
   * Row type of the input.
   */
  private final RowType rowType;

  /**
   * Avro schema of the input.
   */
  private transient Schema avroSchema;

  /**
   * RowData to Avro record converter.
   */
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;

  /**
   * HoodieKey generator.
   */
  private transient KeyGenerator keyGenerator;

  /**
   * Utilities to create hoodie pay load instance.
   */
  private transient PayloadCreation payloadCreation;

  /**
   * Config options.
   */
  private final Configuration config;

  public RowDataToHoodieFunction(RowType rowType, Configuration config) {
    this.rowType = rowType;
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType);
    this.keyGenerator =
        HoodieAvroKeyGeneratorFactory
            .createKeyGenerator(flinkConf2TypedProperties(this.config));
    this.payloadCreation = PayloadCreation.instance(config);
  }

  @SuppressWarnings("unchecked")
  @Override
  public O map(I i) throws Exception {
    return (O) toHoodieRecord(i);
  }

  /**
   * Converts the give record to a {@link HoodieRecord}.
   *
   * @param record The input record
   * @return HoodieRecord based on the configuration
   * @throws IOException if error occurs
   */
  @SuppressWarnings("rawtypes ")
  //todo RowData 2 HoodieRecord
  private HoodieRecord toHoodieRecord(I record) throws Exception {
    //todo 根据AvroSchema，将RowData数据转换为Avro格式
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    //todo 获取HoodieKey，它由record key字段值和partitionPath（分区路径）共同确定
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);
    //todo 创建数据载体，该对象包含RowData数据
    HoodieRecordPayload payload = payloadCreation.createPayload(gr);
    //todo 获取操作类型，增删改查
    HoodieOperation operation = HoodieOperation.fromValue(record.getRowKind().toByteValue());
    //todo 构造出HoodieRecord
    return new HoodieAvroRecord<>(hoodieKey, payload, operation);
  }
}
