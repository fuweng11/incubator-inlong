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

import React, { useRef, useState, useEffect } from 'react';
import { useHistory, useParams, useLocation } from 'react-router-dom';
import { Button, Card, Modal, message } from '@tencent/tea-component';
import { PageContainer, Container, FooterToolbar } from '@/@tencent/components/PageContainer';
import request from '@/core/utils/request';
import { useRequest } from 'ahooks';
import { parse } from 'qs';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { SourceTypeEnum, sourceTypeApiPathMap } from '@/@tencent/enums/source';
import BasicForm, { BasicFormRef } from './BasicForm';
import AccessForm, { AccessFormRef } from './AccessForm';

export default function StreamCreate() {
  const { id: streamId } = useParams<{ id: string }>();

  const [projectId] = useProjectId();

  const location = useLocation();

  const { sourceType } = parse(location.search.slice(1));

  const basicFormRef = useRef<BasicFormRef>();
  const accessFormRef = useRef<AccessFormRef>();

  const [loading, setLoading] = useState<boolean>(false);
  const [changed, setChanged] = useState<boolean>(false);

  const { data: savedData } = useRequest(
    {
      url: sourceTypeApiPathMap.get(sourceType as SourceTypeEnum)
        ? `/access/${sourceTypeApiPathMap.get(sourceType as SourceTypeEnum)}/query`
        : '/access/query/info',
      method: 'POST',
      data: {
        projectID: projectId,
        streamID: streamId,
      },
    },
    {
      ready: Boolean(streamId),
      refreshDeps: [streamId],
    },
  );

  const history = useHistory();

  const submit = async () => {
    setLoading(true);
    try {
      const [basicV, accessV] = await Promise.all([
        basicFormRef.current.submit(),
        accessFormRef.current.submit(),
      ]);
      const values = { ...basicV, ...accessV };
      console.log('values: ', values);
      const path =
        sourceTypeApiPathMap.get(values.accessModel) ||
        sourceTypeApiPathMap.get(values.sourceType) ||
        values.accessModel;
      await request({
        url: `/access/${path}/create`,
        method: 'POST',
        data: {
          ...values,
          projectID: projectId,
        },
      });
      history.block(() => null);
      history.push('/stream');
      message.success({ content: '新建成功' });
    } catch (err) {
      console.warn(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const unblock = history.block(location => {
      if (changed) {
        Modal.confirm({
          message: '取消新建接入',
          description: '取消新建接入，所有编辑数据将丢失，确定取消新建接入操作吗？',
          onOk: () => {
            unblock();
            history.push(location);
          },
        });
        return false;
      }
      return;
    });

    const beforeunload = event => {
      if (changed) {
        event.preventDefault();
        event.returnValue = '';
      }
    };
    window.addEventListener('beforeunload', beforeunload);

    return () => {
      unblock();
      window.removeEventListener('beforeunload', beforeunload);
    };
  }, [changed, history]);

  return (
    <PageContainer useDefaultContainer={false} breadcrumb={[{ name: '新建接入' }]}>
      <Container useDefaultBackground={false}>
        <Card>
          <Card.Body title="基本信息">
            <BasicForm
              ref={basicFormRef}
              isUpdate={Boolean(streamId)}
              savedData={savedData}
              onChange={() => !changed && setChanged(true)}
            />
          </Card.Body>
        </Card>
      </Container>

      <Container useDefaultBackground={false}>
        <Card>
          <Card.Body title="接入信息">
            <AccessForm
              ref={accessFormRef}
              isUpdate={Boolean(streamId)}
              savedData={savedData}
              onChange={() => !changed && setChanged(true)}
            />
          </Card.Body>
        </Card>
      </Container>

      <FooterToolbar align="right">
        <Button type="primary" style={{ marginRight: 10 }} loading={loading} onClick={submit}>
          新建
        </Button>
        <Button onClick={() => history.push('/stream')}>取消</Button>
      </FooterToolbar>
    </PageContainer>
  );
}
