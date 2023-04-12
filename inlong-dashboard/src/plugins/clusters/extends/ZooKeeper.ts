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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import { ClusterInfo } from '../common/ClusterInfo';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

export default class ZooKeeperCluster
  extends ClusterInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    initialValue: '127.0.0.1:9092',
  })
  @I18n('URL')
  url: string;

  @FieldDecorator({
    type: 'input',
  })
  @I18n('tubeRoot')
  tubeRoot: string;

  @FieldDecorator({
    type: 'input',
  })
  @I18n('pulsarRoot')
  pulsarRoot: string;

  @FieldDecorator({
    type: 'input',
  })
  @I18n('kafkaRoot')
  kafkaRoot: string;
}