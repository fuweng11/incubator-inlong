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
import { SourceInfo } from '../common/SourceInfo';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class MongodbSource
  extends SourceInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
      placeholder: 'localhost:27017,localhost:27018',
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @I18n('meta.Sources.Mongodb.Hosts')
  hosts: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @I18n('meta.Sources.Mongodb.Username')
  username: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @I18n('meta.Sources.Mongodb.Password')
  password: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @I18n('meta.Sources.Mongodb.Database')
  database: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @I18n('meta.Sources.Mongodb.Collection')
  collection: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @I18n('meta.Sources.Mongodb.PrimaryKey')
  primaryKey: string;
}
