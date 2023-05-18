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
// import { MQTypeEnum, mqTypeMap } from '@/@tencent/enums/subscribe/mq';
// import UserSelect from '@/@tencent/components/UserSelect';
import { SubscribeFormProps, SubscribeFormRef } from './common';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { useSelector } from 'react-redux';
import { State } from '@/core/stores';

export { fields, MQTypeEnum } from '@/@tencent/enums/subscribe/mq';

const MQ = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const {
    fields,
    streamInfo: { inLongGroupID },
    setTargetFields,
    ...rest
  } = props;

  const formRef = useRef<Form>();
  const [projectId] = useProjectId();
  const userName = useSelector<State, State['userName']>(state => state.userName);

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'mqType',
      type: 'string',
      title: 'MQ类型',
      required: true,
      component: 'input',
      readonly: true,
    },
    {
      name: 'topic',
      type: 'string',
      title: 'topic',
      component: 'select',
      required: true,
      appearance: 'button',
      size: 'm',
      reaction: async (field, values) => {
        const data = await request({
          url: '/access/topic/query',
          method: 'POST',
          data: {
            projectID: projectId,
            inLongGroupID,
          },
        });
        formRef.current?.setValues({ mqType: data.mqType });
        field.setComponentProps({
          options: (data?.topics || []).map(t => ({ text: t, value: t })),
        });
        field.setValue(data.topics[0]);
      },
    },
    {
      name: 'consumer',
      type: 'string',
      title: '消费组名称',
      required: false,
      component: 'input',
      readonly: true,
    },
    {
      name: 'cluster',
      type: 'string',
      title: '集群信息',
      required: false,
      component: 'input',
      readonly: true,
    },
  ]);

  const submit = useCallback(async () => {
    const [basicForm] = await Promise.all([formRef.current.submit() as Record<string, any>]);
    return {
      ...basicForm,
      inCharges: userName,
    };
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    fieldsMapProps: {},
  }));

  return (
    <>
      <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />
      <p style={{ marginTop: 16, color: '#00000044' }}>提示：消费组，集群信息在审批通过后生效</p>
    </>
  );
});

export default MQ;
