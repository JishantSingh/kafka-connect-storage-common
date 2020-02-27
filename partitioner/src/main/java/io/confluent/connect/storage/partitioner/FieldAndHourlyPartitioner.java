/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class FieldAndHourlyPartitioner<T> extends TimeBasedPartitioner<T> {
  private HourlyPartitioner<T> hourlyPartitioner = new HourlyPartitioner<>();
  private FieldPartitioner<T> fieldPartitioner = new FieldPartitioner<>();
  private static final Logger log = LoggerFactory.getLogger(TimeBasedPartitioner.class);

  @Override
  public void configure(Map<String, Object> config) {
    delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    hourlyPartitioner.configure(config);
    fieldPartitioner.configure(config);
    super.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return fieldPartitioner.encodePartition(sinkRecord)
        + delim + hourlyPartitioner.encodePartition(sinkRecord);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    return fieldPartitioner.encodePartition(sinkRecord)
        + delim + hourlyPartitioner.encodePartition(sinkRecord, nowInMillis);
  }

  @Override
  public List<T> partitionFields() {
    List<T> fieldPartitionerPartitionFields = fieldPartitioner.partitionFields();
    fieldPartitionerPartitionFields.addAll(hourlyPartitioner.partitionFields);
    return fieldPartitionerPartitionFields;

  }
}
