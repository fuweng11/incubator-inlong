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
import Description from '@/@tencent/components/Description';

export default function Info() {
  return (
    <Description column={3}>
      <Description.Item title="接入方式">SDK</Description.Item>
      <Description.Item title="单日峰值">2022-01-01</Description.Item>
      <Description.Item title="单日最大接入量">aa</Description.Item>
      <Description.Item title="单条数据最大值">bb</Description.Item>
      <Description.Item title="采集类型">cc</Description.Item>
    </Description>
  );
}
