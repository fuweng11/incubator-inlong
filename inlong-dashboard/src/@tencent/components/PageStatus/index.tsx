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

import React, { ReactNode } from 'react';

interface PageStatusProps {
  icon: ReactNode;
  description: ReactNode | string;
}

const style: React.CSSProperties = {
  maxWidth: 400,
  margin: '0 auto',
  padding: '120px 0',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  flexDirection: 'column',
  fontSize: '12',
};

export default function PageStatus({ icon, description }: PageStatusProps) {
  return (
    <div style={style}>
      {icon}
      <div style={{ marginTop: 16 }}>{description}</div>
    </div>
  );
}
