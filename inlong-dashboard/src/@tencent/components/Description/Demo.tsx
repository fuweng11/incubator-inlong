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

import React, { useState } from 'react';
import { Button, Modal } from '@tencent/tea-component';
import Description from '.';

export default () => {
  const [visible, setVisible] = useState(false);

  return (
    <>
      <Button onClick={() => setVisible(true)}>打开展示Description描述组件</Button>

      <Modal size="l" visible={visible} onClose={() => setVisible(false)}>
        <Modal.Body>
          <Description column={4} title="Description Title" extra={<Button>extra</Button>}>
            <Description.Item title="名称">name</Description.Item>
            <Description.Item title="创建人">creator</Description.Item>
            <Description.Item title="创建时间">creatTime</Description.Item>
            <Description.Item title="描述" span={3}>
              remark
            </Description.Item>
          </Description>
        </Modal.Body>
        <Modal.Footer>
          <Button type="primary" onClick={() => setVisible(false)}>
            关闭
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
};
