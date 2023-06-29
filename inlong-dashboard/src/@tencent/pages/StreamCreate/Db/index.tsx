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

import React from 'react';
import { Form, Select } from '@tencent/tea-component';
import { Controller } from 'react-hook-form';
import { DbTypeEnum, dbTypeMap } from '@/@tencent/enums/source';
import Mysql from './Mysql';
import PostgreSql from './PostgreSql';
import { useParams } from 'react-router-dom';

export default function Db({ form }) {
  const { control, formState, watch, setValue } = form;
  const { errors } = formState;
  const watchDbType = watch('sourceType', DbTypeEnum.MySQL);

  const { id: streamId } = useParams<{ id: string }>();

  return (
    <>
      <Form.Item
        label="数据源类型"
        align="middle"
        required
        status={errors.sourceType?.message ? 'error' : undefined}
        message={errors.sourceType?.message}
      >
        <Controller
          name="sourceType"
          defaultValue={DbTypeEnum.MySQL}
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据源类型' }}
          render={({ field }) => (
            <Select
              {...(field as any)}
              style={{ minWidth: 200 }}
              appearance="button"
              options={Array.from(dbTypeMap).map(([key, ctx]) => ({
                value: key,
                text: ctx,
              }))}
              onChange={v => {
                setValue('sourceType', v);
                setValue('dataSourceID', undefined);
              }}
              disabled={!!streamId}
            />
          )}
        />
      </Form.Item>

      {(() => {
        const compMap = {
          [DbTypeEnum.MySQL]: Mysql,
          [DbTypeEnum.PostgreSQL]: PostgreSql,
        };
        const Comp = compMap[watchDbType];
        return Comp ? <Comp form={form} /> : null;
      })()}
    </>
  );
}
