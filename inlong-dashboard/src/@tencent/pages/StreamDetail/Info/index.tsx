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

import React, { useMemo } from 'react';
import { Form, Table, StatusTip } from '@tencent/tea-component';
import ReadonlyForm from '@/@tencent/components/ReadonlyForm';
import { getFields } from './fields';
import { SourceTypeEnum } from '@/@tencent/enums/source';

const Info = ({ streamId, info }) => {
  const accessModel = info.accessModel;

  const conf = useMemo(() => {
    return getFields(accessModel);
  }, [accessModel]);

  // 临时给PG的读取方式补上展示的默认值(后台不返回这个字段，但又需要前端展示)
  const finalInfo =
    accessModel === SourceTypeEnum.PostgreSQL ? { ...info, readType: '全量' } : info;

  return (
    <ReadonlyForm
      conf={conf}
      defaultValues={finalInfo}
      footer={
        <Form.Item label="数据字段" align="middle">
          <Table
            bordered
            verticalTop
            records={finalInfo?.fieldsData || []}
            recordKey="fieldName"
            columns={[
              {
                key: 'fieldName',
                header: '字段名',
              },
              {
                key: 'fieldType',
                header: '字段类型',
              },
              {
                key: 'remark',
                header: '字段描述',
              },
            ]}
            topTip={!finalInfo?.fieldsData?.length && <StatusTip status="empty" />}
          />
        </Form.Item>
      }
    />
  );
};

export default Info;
