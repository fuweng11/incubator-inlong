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
import { SubscribeFormProps, SubscribeFormRef } from './common';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';

export { fields } from '@/@tencent/enums/subscribe/clickhouse';

const Clickhouse = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const { fields, streamInfo, setTargetFields, ...rest } = props;

  const formRef = useRef<Form>();

  const [projectId] = useProjectId();

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'inLongNodeName',
      type: 'string',
      title: '数据源',
      component: 'select',
      appearance: 'button',
      size: 'm',
      reaction: async (field, values) => {
        const data = await request({
          url: '/datasource/search',
          method: 'POST',
          data: {
            projectID: projectId,
            type: 'CLICKHOUSE',
            pageSize: 99,
            pageNum: 0,
          },
        });
        field.setComponentProps({
          options: (data?.records || []).map(item => ({
            text: item.displayName,
            value: item.name,
          })),
        });
      },
    },
    {
      name: 'dbName',
      type: 'string',
      title: '库名',
      component: 'input',
      required: true,
    },
    {
      name: 'tableName',
      type: 'string',
      title: '表名',
      component: 'input',
      required: true,
    },
  ]);

  const submit = useCallback(async () => {
    const [basicForm] = await Promise.all([formRef.current.submit() as object]);
    return basicForm;
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    fieldsMapProps: {},
  }));

  return <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />;
});

export default Clickhouse;
