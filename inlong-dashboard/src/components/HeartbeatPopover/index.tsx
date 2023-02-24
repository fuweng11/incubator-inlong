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
import { Popover } from 'antd';
import { useRequest } from '@/hooks';
import { timestampFormat } from '@/utils';
import i18n from '@/i18n';

export interface Props {
  id: string;
  dataNodeName: string;
}

const fieldsConf = {
  instance: i18n.t('components.HeartbeatPopover.Instance'),
  currentDb: i18n.t('components.HeartbeatPopover.CurrentDb'),
  url: i18n.t('components.HeartbeatPopover.Url'),
  backupUrl: i18n.t('components.HeartbeatPopover.BackupUrl'),
  agentStatus: text =>
    i18n.t('components.HeartbeatPopover.AgentStatus') +
    `${
      {
        NONE: i18n.t('components.HeartbeatPopover.AgentStatus.None'),
        NORMAL: i18n.t('components.HeartbeatPopover.AgentStatus.Normal'),
        STOPPED: i18n.t('components.HeartbeatPopover.AgentStatus.Stopped'),
        SWITCHED: i18n.t('components.HeartbeatPopover.AgentStatus.Switched'),
      }[text] || ''
    }`,
  dumpPosition: (text, record) => {
    const data = record?.dumpPosition?.entryPosition;
    if (data) {
      return (
        i18n.t('components.HeartbeatPopover.DumpPosition') +
        i18n.t('components.HeartbeatPopover.DumpPosition.JournalName') +
        `=${data.journalName},` +
        i18n.t('components.HeartbeatPopover.DumpPosition.Position') +
        `=${data.position},` +
        i18n.t('components.HeartbeatPopover.DumpPosition.Included') +
        `=${data.included},` +
        i18n.t('components.HeartbeatPopover.DumpPosition.Timestamp') +
        `=${data.timestamp ? timestampFormat(data.timestamp) : ''}`
      );
    }
  },
  maxLogPosition: (text, record) => {
    const data = record?.maxLogPosition?.entryPosition;
    if (data) {
      return (
        i18n.t('components.HeartbeatPopover.MaxLogPosition') +
        i18n.t('components.HeartbeatPopover.MaxLogPosition.JournalName') +
        `=${data.journalName}, ` +
        i18n.t('components.HeartbeatPopover.MaxLogPosition.Position') +
        `=${data.position}`
      );
    }
  },
  errorMsg: i18n.t('components.HeartbeatPopover.errorMsg'),
};

const Comp: React.FC<Props> = ({ id, dataNodeName, children }) => {
  const { data, run: getData } = useRequest(
    {
      url: `/dbsync/getHeartbeat`,
      method: 'POST',
      params: {
        id,
        dataNodeName,
      },
    },
    {
      manual: true,
    },
  );

  const onVisibleChange = visible => {
    if (visible) getData();
  };

  return (
    <Popover
      placement="topRight"
      overlayInnerStyle={{ width: 400 }}
      content={
        data &&
        Object.keys(fieldsConf).map(key => {
          const config = fieldsConf[key];
          const text =
            typeof config === 'string' ? `${config}${data[key]}` : config(data[key], data);
          return <div>{text}</div>;
        })
      }
      onVisibleChange={onVisibleChange}
    >
      {children}
    </Popover>
  );
};

export default Comp;
