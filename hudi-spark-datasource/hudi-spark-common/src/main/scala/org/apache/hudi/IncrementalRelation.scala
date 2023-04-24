/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieFileFormat, HoodieRecord, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}

import java.util.stream.Collectors
import org.apache.hadoop.fs.{GlobPattern, Path}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{HoodieTimer, InternalSchemaCache}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.table.HoodieSparkTable
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Relation, that implements the Hoodie incremental view.
 *
 * Implemented for Copy_on_write storage.
 *
 * todo 增量读取入口
 *
 */
class IncrementalRelation(val sqlContext: SQLContext,
                          val optParams: Map[String, String],
                          val userSchema: Option[StructType],
                          val metaClient: HoodieTableMetaClient) extends BaseRelation with TableScan {

  private val log = LogManager.getLogger(classOf[IncrementalRelation])

  val skeletonSchema: StructType = HoodieSparkUtils.getMetaSchema
  private val basePath = metaClient.getBasePath
  // TODO : Figure out a valid HoodieWriteConfig
  //todo 创建hudi表
  private val hoodieTable = HoodieSparkTable.create(HoodieWriteConfig.newBuilder().withPath(basePath).build(),
    new HoodieSparkEngineContext(new JavaSparkContext(sqlContext.sparkContext)),
    metaClient)
  private val commitTimeline = hoodieTable.getMetaClient.getCommitTimeline.filterCompletedInstants()
  if (commitTimeline.empty()) {
    throw new HoodieException("No instants to incrementally pull")
  }
  if (!optParams.contains(DataSourceReadOptions.BEGIN_INSTANTTIME.key)) {
    throw new HoodieException(s"Specify the begin instant time to pull from using " +
      s"option ${DataSourceReadOptions.BEGIN_INSTANTTIME.key}")
  }
  if (!metaClient.getTableConfig.populateMetaFields()) {
    throw new HoodieException("Incremental queries are not supported when meta fields are disabled")
  }

  val useEndInstantSchema = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key,
    DataSourceReadOptions.INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue).toBoolean

  private val lastInstant = commitTimeline.lastInstant().get()
  //todo 获取【begintime，endtime】之间内的committimeline
  private val commitsTimelineToReturn = commitTimeline.findInstantsInRange(
    optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key),
    optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(), lastInstant.getTimestamp))
  //todo 获取【begintime，endtime】之间内的committimeline的instants
  private val commitsToReturn = commitsTimelineToReturn.getInstants.iterator().toList

  // use schema from a file produced in the end/latest instant

  val (usedSchema, internalSchema) = {
    log.info("Inferring schema..")
    val schemaResolver = new TableSchemaResolver(metaClient)
    //todo 获取iSchema
    val iSchema = if (useEndInstantSchema && !commitsToReturn.isEmpty) {
      InternalSchemaCache.searchSchemaAndCache(commitsToReturn.last.getTimestamp.toLong, metaClient, hoodieTable.getConfig.getInternalSchemaCacheEnable)
    } else {
      schemaResolver.getTableInternalSchemaFromCommitMetadata.orElse(null)
    }

    val tableSchema = if (useEndInstantSchema && iSchema.isEmptySchema) {
      if (commitsToReturn.isEmpty) schemaResolver.getTableAvroSchemaWithoutMetadataFields() else
        schemaResolver.getTableAvroSchemaWithoutMetadataFields(commitsToReturn.last)
    } else {
      schemaResolver.getTableAvroSchemaWithoutMetadataFields()
    }
    if (tableSchema.getType == Schema.Type.NULL) {
      // if there is only one commit in the table and is an empty commit without schema, return empty RDD here
      (StructType(Nil), InternalSchema.getEmptyInternalSchema)
    } else {
      val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
      if (iSchema != null && !iSchema.isEmptySchema) {
        // if internalSchema is ready, dataSchema will contains skeletonSchema
        (dataSchema, iSchema)
      } else {
        (StructType(skeletonSchema.fields ++ dataSchema.fields), InternalSchema.getEmptyInternalSchema)
      }
    }
  }

  private val filters = optParams.getOrElse(DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS.key,
    DataSourceReadOptions.PUSH_DOWN_INCR_FILTERS.defaultValue).split(",").filter(!_.isEmpty)

  override def schema: StructType = usedSchema

  //todo 读取数据
  override def buildScan(): RDD[Row] = {
    if (usedSchema == StructType(Nil)) {
      // if first commit in a table is an empty commit without schema, return empty RDD here
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val regularFileIdToFullPath = mutable.HashMap[String, String]()
      var metaBootstrapFileIdToFullPath = mutable.HashMap[String, String]()

      // create Replaced file group
      val replacedTimeline = commitsTimelineToReturn.getCompletedReplaceTimeline
      //todo ReplaceTimeline上<replacefileid,partitionpath>的对应关系
      val replacedFile = replacedTimeline.getInstants.collect(Collectors.toList[HoodieInstant]).flatMap { instant =>
        //todo 将instant对应的文件转化为HoodieReplaceCommitMetadata
        val replaceMetadata = HoodieReplaceCommitMetadata.
          fromBytes(metaClient.getActiveTimeline.getInstantDetails(instant).get, classOf[HoodieReplaceCommitMetadata])
        replaceMetadata.getPartitionToReplaceFileIds.entrySet().flatMap { entry =>
          entry.getValue.map { e =>
            val fullPath = FSUtils.getPartitionPath(basePath, entry.getKey).toString
            //todo (replacefileid,partitionpath全路径)
            (e, fullPath)
          }
        }
      }.toMap
      //todo 区间范围内的commit
      for (commit <- commitsToReturn) {
        val metadata: HoodieCommitMetadata = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit)
          .get, classOf[HoodieCommitMetadata])

        if (HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS == commit.getTimestamp) {
          metaBootstrapFileIdToFullPath ++= metadata.getFileIdAndFullPaths(basePath).toMap.filterNot { case (k, v) =>
            //todo 去除replacedFile
            replacedFile.contains(k) && v.startsWith(replacedFile(k))
          }
        } else {
          //todo 过滤掉replacedFile【老fileid】,避免重复读取
          regularFileIdToFullPath ++= metadata
            .getFileIdAndFullPaths(basePath).toMap.filterNot { case (k, v) =>
            replacedFile.contains(k) && v.startsWith(replacedFile(k))
          }
        }
      }

      if (metaBootstrapFileIdToFullPath.nonEmpty) {
        // filer out meta bootstrap files that have had more commits since metadata bootstrap
        metaBootstrapFileIdToFullPath = metaBootstrapFileIdToFullPath
          .filterNot(fileIdFullPath => regularFileIdToFullPath.contains(fileIdFullPath._1))
      }

      val pathGlobPattern = optParams.getOrElse(
        DataSourceReadOptions.INCR_PATH_GLOB.key,
        DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)
      val (filteredRegularFullPaths, filteredMetaBootstrapFullPaths) = {
        if (!pathGlobPattern.equals(DataSourceReadOptions.INCR_PATH_GLOB.defaultValue)) {
          val globMatcher = new GlobPattern("*" + pathGlobPattern)
          (regularFileIdToFullPath.filter(p => globMatcher.matches(p._2)).values,
            metaBootstrapFileIdToFullPath.filter(p => globMatcher.matches(p._2)).values)
        } else {
          //todo
          (regularFileIdToFullPath.values, metaBootstrapFileIdToFullPath.values)
        }
      }
      // unset the path filter, otherwise if end_instant_time is not the latest instant, path filter set for RO view
      // will filter out all the files incorrectly.
      // pass internalSchema to hadoopConf, so it can be used in executors.
      val validCommits = metaClient
        .getCommitsAndCompactionTimeline.filterCompletedInstants.getInstants.toArray().map(_.asInstanceOf[HoodieInstant].getFileName).mkString(",")
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchema))
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, metaClient.getBasePath)
      sqlContext.sparkContext.hadoopConfiguration.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
      val formatClassName = metaClient.getTableConfig.getBaseFileFormat match {
        case HoodieFileFormat.PARQUET => HoodieParquetFileFormat.FILE_FORMAT_ID
        case HoodieFileFormat.ORC => "orc"
      }
      sqlContext.sparkContext.hadoopConfiguration.unset("mapreduce.input.pathFilter.class")
      val sOpts = optParams.filter(p => !p._1.equalsIgnoreCase("path"))
      if (filteredRegularFullPaths.isEmpty && filteredMetaBootstrapFullPaths.isEmpty) {
        sqlContext.sparkContext.emptyRDD[Row]
      } else {
        log.info("Additional Filters to be applied to incremental source are :" + filters.mkString("Array(", ", ", ")"))

        var df: DataFrame = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], usedSchema)

        val fallbackToFullTableScan = optParams.getOrElse(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.key,
          DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES.defaultValue).toBoolean

        var doFullTableScan = false

        if (fallbackToFullTableScan) {
          val fs = new Path(basePath).getFileSystem(sqlContext.sparkContext.hadoopConfiguration);
          val timer = new HoodieTimer().startTimer();

          val allFilesToCheck = filteredMetaBootstrapFullPaths ++ filteredRegularFullPaths
          val firstNotFoundPath = allFilesToCheck.find(path => !fs.exists(new Path(path)))
          val timeTaken = timer.endTimer()
          log.info("Checking if paths exists took " + timeTaken + "ms")

          val optStartTs = optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)
          val isInstantArchived = optStartTs.compareTo(commitTimeline.firstInstant().get().getTimestamp) < 0 // True if optStartTs < activeTimeline.first

          if (isInstantArchived || firstNotFoundPath.isDefined) {
            doFullTableScan = true
            log.info("Falling back to full table scan")
          }
        }

        if (doFullTableScan) {
          val hudiDF = sqlContext.read
            .format("hudi_v1")
            .schema(usedSchema)
            .load(basePath)
            .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, //Notice the > in place of >= because we are working with optParam instead of first commit > optParam
              optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)))
            .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              commitsToReturn.last.getTimestamp))
          // schema enforcement does not happen in above spark.read with hudi. hence selecting explicitly w/ right column order
          val fieldNames : Array[String] = df.schema.fields.map(field => field.name)
          df = df.union(hudiDF.select(fieldNames.head, fieldNames.tail: _*))
        } else {
          if (metaBootstrapFileIdToFullPath.nonEmpty) {
            df = sqlContext.sparkSession.read
              .format("hudi_v1")
              .schema(usedSchema)
              .option(DataSourceReadOptions.READ_PATHS.key, filteredMetaBootstrapFullPaths.mkString(","))
              .load()
          }

          if (regularFileIdToFullPath.nonEmpty) {
            df = df.union(sqlContext.read.options(sOpts)
              .schema(usedSchema).format(formatClassName)
               //todo 读取filteredRegularFullPaths中的数据！！！
              .load(filteredRegularFullPaths.toList: _*)
              //todo 增量查询的本质是用_hoodie_commit_time做了过滤！！！
              .filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                commitsToReturn.head.getTimestamp))
              .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                commitsToReturn.last.getTimestamp)))
          }
        }

        filters.foldLeft(df)((e, f) => e.filter(f)).rdd
      }
    }
  }
}
