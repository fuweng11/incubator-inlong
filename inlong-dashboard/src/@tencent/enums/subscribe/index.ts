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

export enum SinkTypeEnum {
  Thive = 'INNER_THIVE',
  Hive = 'INNER_HIVE',
  Clickhouse = 'CLICKHOUSE',
  Hudi = 'HUDI',
  Kafka = 'KAFKA',
  MQ = 'MQ',
  Iceberg = 'INNER_ICEBERG',
}

export const sinkTypeMap: Map<SinkTypeEnum, string> = (() => {
  return new Map([
    [SinkTypeEnum.Thive, 'Thive'],
    [SinkTypeEnum.Hive, 'Hive'],
    [SinkTypeEnum.Clickhouse, 'Clickhouse'],
    [SinkTypeEnum.Hudi, 'Hudi'],
    [SinkTypeEnum.Iceberg, 'Iceberg'],
    [SinkTypeEnum.Kafka, 'Kafka'],
    [SinkTypeEnum.MQ, 'MQ'],
  ]);
})();

export const sinkTypeApiPathMap: Map<SinkTypeEnum, string> = (() => {
  return new Map([
    [SinkTypeEnum.Thive, 'thive'],
    [SinkTypeEnum.Hive, 'hive'],
    [SinkTypeEnum.Clickhouse, 'clickhouse'],
    [SinkTypeEnum.Hudi, 'hudi'],
    [SinkTypeEnum.Iceberg, 'iceberg'],
    [SinkTypeEnum.Kafka, 'kafka'],
    [SinkTypeEnum.MQ, 'innermq'],
  ]);
})();
