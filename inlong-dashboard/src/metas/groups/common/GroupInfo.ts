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

import { GroupDefaultInfo } from './GroupDefaultInfo';
import { DataWithBackend } from '@/metas/DataWithBackend';
import { RenderRow } from '@/metas/RenderRow';
import { RenderList } from '@/metas/RenderList';
import ProductSelect from '@/components/ProductSelect';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;
const { ColumnDecorator } = RenderList;

export class GroupInfo extends GroupDefaultInfo {
  // You can extends GroupInfo at here...
  @FieldDecorator({
    type: ProductSelect,
    extraNames: ['productName'],
    rules: [{ required: true }],
    props: values => ({
      asyncValueLabel: values.productName,
      disabled: [110, 130].includes(values?.status),
      onChange: (value, record) => ({
        appGroupName: undefined,
        productName: record.name,
      }),
    }),
  })
  @I18n('meta.Group.Product')
  productId: string | number;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      allowClear: true,
      disabled: [110, 130].includes(values?.status),
      options: {
        requestService: {
          url: '/sc/appgroup/my',
          params: {
            productId: values.productId,
          },
        },
        requestParams: {
          formatResult: result =>
            result?.map(item => ({
              label: item,
              value: item,
            })),
        },
      },
    }),
  })
  @I18n('meta.Group.AppGroupName')
  appGroupName: string;
}
