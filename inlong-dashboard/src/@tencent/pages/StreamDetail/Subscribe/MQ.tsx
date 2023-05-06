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

import React, { useRef, useCallback, forwardRef, useImperativeHandle, Ref } from 'react';
import { ProForm, ProFormProps, Form } from '@tencent/tea-material-pro-form';
import { MQTypeEnum, mqTypeMap } from '@/@tencent/enums/subscribe/mq';
import { SubscribeFormProps, SubscribeFormRef } from './common';

const Kafka = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const { fields, streamInfo, setTargetFields, ...rest } = props;

  const formRef = useRef<Form>();

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'mqType',
      type: 'string',
      title: 'MQ类型',
      required: true,
      component: 'radio',
      defaultValue: MQTypeEnum.PULSAR,
      options: Array.from(mqTypeMap).map(([key, ctx]) => ({ name: key, title: ctx })),
    },
    {
      name: 'topic',
      type: 'string',
      title: 'topic',
      required: true,
      component: 'input',
    },
    {
      name: 'inCharges',
      type: 'string',
      title: '负责人列表',
      required: true,
      component: 'input',
      placeholder: '逗号分隔',
    },
  ]);

  const submit = useCallback(async () => {
    const [basicForm] = await Promise.all([formRef.current.submit() as Record<string, any>]);
    return basicForm;
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    fieldsMapProps: {},
  }));

  return <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />;
});

export default Kafka;
