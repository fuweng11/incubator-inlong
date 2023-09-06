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

import { ConsumeDefaultInfo } from './ConsumeDefaultInfo';
import { RenderRow } from '@/plugins/RenderRow';
import { DataWithBackend } from '@/plugins/DataWithBackend';
import i18n from '@/i18n';
import ProductSelect from '@/ui/components/ProductSelect';
import config from './Config';
import dayjs from 'dayjs';

const { FieldDecorator } = RenderRow;
const { I18nMap, I18n } = DataWithBackend;

export class ConsumeInfo extends ConsumeDefaultInfo {
  // You can extends ConsumeInfo at here...
  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      requestTrigger: ['onOpen', 'onSearch'],
      disabled: [110, 130].includes(values?.status),
      options: {
        requestService: {
          url: '/bglist',
          method: 'GET',
        },
        requestParams: {
          formatResult: result =>
            result?.map(item => ({
              label: item.name,
              value: item.abbrevName,
            })),
        },
      },
    }),
  })
  @I18n('meta.Consume.Bg')
  bg: string;

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
  @I18n('meta.Consume.Product')
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
  @I18n('meta.Consume.ResourceAppGroupName')
  resourceAppGroupName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: {
      options: config.UsageOptions,
    },
  })
  @I18n('meta.Consume.DataUsage')
  usageType: string;

  @FieldDecorator({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 100,
    },
  })
  @I18n('meta.Consume.DataUsageDesc')
  usageDesc: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 0,
    props: {
      options: [
        {
          label: i18n.t('meta.Consume.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Consume.No'),
          value: 0,
        },
      ],
    },
    rules: [{ required: true }],
  })
  @I18n('meta.Consume.ConfigureAlarm')
  alertEnabled: number;

  @FieldDecorator({
    type: 'radio',
    props: {
      options: [
        {
          label: i18n.t('meta.Consume.AlertType.SMS'),
          value: 'sms',
        },
        {
          label: i18n.t('meta.Consume.AlertType.Email'),
          value: 'email',
        },
        {
          label: i18n.t('meta.Consume.AlertType.NOC'),
          value: 'noc',
        },
      ],
    },
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
  })
  @I18n('meta.Consume.AlertType')
  alertType: string;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
    suffix: i18n.t('meta.Consume.StartTimes.Unit'),
    props: {
      min: 0,
      precision: 0,
    },
  })
  @I18n('meta.Consume.StartTimes')
  startTimes: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
    suffix: i18n.t('meta.Consume.UpgradeTimes.Unit'),
    props: {
      min: 0,
      precision: 0,
    },
  })
  @I18n('meta.Consume.UpgradeTimes')
  upgradeTimes: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
    suffix: i18n.t('meta.Consume.ResetPeriod.Unit'),
    props: {
      min: 0,
    },
  })
  @I18n('meta.Consume.ResetPeriod')
  resetPeriod: number;

  @FieldDecorator({
    type: 'radio',
    initialValue: 0,
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
    props: {
      options: [
        {
          label: i18n.t('meta.Consume.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Consume.No'),
          value: 0,
        },
      ],
    },
  })
  @I18n('meta.Consume.MaskSwitch')
  maskSwitch: number;

  @FieldDecorator({
    type: 'rangepicker',
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1 && values.maskSwitch === 1,
    props: {
      showTime: {
        hideDisabledOptions: true,
      },
      disabledDate: current => current && current < dayjs().hour(0),
      format: 'YYYY-MM-DD HH:mm',
    },
  })
  @I18n('meta.Consume.MaskTime')
  maskTime: string;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    visible: values => values.alertEnabled === 1,
    suffix: i18n.t('meta.Consume.Threshold.Unit'),
    props: {
      min: 0,
    },
  })
  @I18n('meta.Consume.Threshold')
  threshold: number;
}
