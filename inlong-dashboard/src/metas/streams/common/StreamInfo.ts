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

import { StreamDefaultInfo } from './StreamDefaultInfo';
import { DataWithBackend } from '@/metas/DataWithBackend';
import i18n from '@/i18n';

const { I18n, FormField } = DataWithBackend;

export class StreamInfo extends StreamDefaultInfo {
  // You can extend StreamInfo at here...

  @FormField({
    type: 'radio',
    initialValue: 'CSV',
    tooltip: i18n.t('meta.Stream.DataTypeHelp'),
    props: {
      options: [
        {
          label: 'CSV',
          value: 'CSV',
        },
        {
          label: 'Raw-CSV',
          value: 'RAW_CSV',
        },
        {
          label: 'BinLog',
          value: 'BINLOG',
        },
      ],
    },
    rules: [{ required: true }],
  })
  @I18n('meta.Stream.DataType')
  dataType: string;
}
