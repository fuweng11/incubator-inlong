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
import request from '@/core/utils/request';
import FetchSelect from '.';

export default () => {
  const [value, setValue] = useState('');

  return (
    <FetchSelect
      value={value}
      onChange={v => setValue(v)}
      style={{ width: 200 }}
      request={async () => {
        const result = await request({
          url: '/access/stream/search',
          method: 'POST',
          data: {
            pageNum: 1,
            pageSize: 10,
            projectID: '1',
          },
        });

        return result?.records.map(item => ({
          text: item.name + item.streamID,
          value: item.name + item.streamID,
        }));
      }}
    />
  );
};
