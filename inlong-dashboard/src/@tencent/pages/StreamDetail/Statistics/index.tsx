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
import { Form, Justify, Segment, Table, Badge } from '@tencent/tea-component';
import React, { useState, useEffect } from 'react';
import Charts from '@/ui/components/Charts';
import request from '@/core/utils/request';
import dayjs from 'dayjs';
import { DateRangePicker } from 'tdesign-react';
import 'tdesign-react/es/style/index.css';
import isEmpty from 'lodash/isEmpty';
import {
  StatisticsNameMap,
  StatisticsColorMap,
  StatisticsTypeEnum,
} from '@/@tencent/enums/statistics';

const dimTitleStyle = {
  fontSize: 14,
  display: 'inline-block',
  marginRight: 10,
  fontFamily: 'PingFang SC',
};

const { pageable, scrollable, sortable } = Table.addons;

const Statistics = ({ info }) => {
  const [auditData, setAuditData] = useState([]);
  const [auditList, setAuditList] = useState([]);
  const [segmentValue1, setSegmentValue1] = useState('DAY');
  const [sorts, setSorts] = useState([]);

  const [presets]: any = useState({
    今天: [dayjs(), dayjs()],
    近一周: [dayjs().subtract(6, 'day'), dayjs()],
    // 近一月: [dayjs().subtract(1, 'month'), dayjs()],
  });

  const [range1, setRange1] = useState(['2023-06-20', '2023-06-26']);

  useEffect(() => {
    getAuditList();
  }, [segmentValue1, range1]);

  const getAuditList = async () => {
    const data = await request({
      url: '/audit/list',
      method: 'POST',
      data: {
        endTime: `${range1[1]}`,
        inLongGroupId: info.inLongGroupID,
        inLongStreamId: info.inLongStreamID,
        pageNum: 1,
        pageSize: 9999,
        startTime: `${range1[0]}`,
        timeStaticsDim: segmentValue1,
      },
    });
    const legends = data.filter(d => !isEmpty(d.auditSet));
    setAuditData(legends);
    setAuditList(toTableList(legends));
  };

  const options = [
    { text: '按天', value: 'DAY' },
    { text: '按小时', value: 'HOUR' },
  ];

  const getOption = () => {
    const xAxisData = auditData[0]?.auditSet?.map(r => r.logTs);
    const series = auditData.map(d => ({
      name: StatisticsNameMap.get(d.auditId),
      type: 'line',
      data: d?.auditSet?.map(r => r.count),
    }));

    return {
      title: {
        text: null,
      },
      tooltip: {
        trigger: 'axis',
      },
      legend: {
        data: auditData.map(d => StatisticsNameMap.get(d.auditId)),
        top: 0,
        right: 0,
        type: 'scroll',
      },
      grid: {
        left: '4%',
        right: '4%',
        bottom: 50,
        containLabel: true,
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: xAxisData,
      },
      yAxis: {
        type: 'value',
      },
      dataZoom: [
        {
          type: 'inside',
          start: 0,
          end: 100,
        },
        {
          start: 0,
          end: 100,
        },
      ],
      color: auditData.map(d => StatisticsColorMap.get(d.auditId)),
      series,
    };
  };

  const toTableList = data => {
    let ret = [];
    let id = 0;
    data.forEach(d => {
      ret = [...ret, ...d.auditSet.map(a => ({ ...a, name: d.auditId, id: id++ }))];
    });

    return ret;
  };

  return (
    <Form>
      <Justify
        style={{ marginBottom: 15 }}
        left={<h3 style={{ fontSize: 14, fontWeight: 700 }}>数据接入量趋势</h3>}
        right={
          <div>
            <div style={dimTitleStyle}>时间维度:</div>
            <Segment
              style={{ marginTop: -10, marginRight: 10 }}
              value={segmentValue1}
              options={options}
              onChange={value => {
                setSegmentValue1(value);
              }}
            />
            <DateRangePicker
              value={range1}
              presets={presets}
              onChange={(val: any) => setRange1(val)}
              disableDate={{
                before: dayjs().subtract(7, 'day').format(),
                after: dayjs().add(0, 'day').format(),
              }}
            />
          </div>
        }
      />

      <Charts height={500} option={getOption()} />
      <Form.Title>数据接入量</Form.Title>
      <Table
        bordered
        verticalTop
        records={[...auditList].sort(sortable.comparer(sorts))}
        recordKey="id"
        addons={[
          pageable(),
          scrollable({ maxHeight: 480, scrollToTopOnChange: true }),
          sortable({
            columns: [
              {
                key: 'logTs',
                prefer: 'desc',
              },
              'input',
              'delay',
              'count',
            ],
            value: sorts,
            onChange: value => setSorts(value),
          }),
        ]}
        columns={[
          {
            key: 'logTs',
            header: '时间',
          },
          {
            key: 'name',
            header: '数据发送/订阅名称',
            render: row => (
              <span style={{ display: 'flex', alignItems: 'center' }}>
                <Badge
                  dot
                  style={{ marginRight: 5, backgroundColor: StatisticsColorMap.get(row.name) }}
                />
                <span>{StatisticsNameMap.get(row.name)}</span>
              </span>
            ),
          },
          {
            key: 'input',
            header: '数据接入量',
            render: row => {
              if (row.name <= StatisticsTypeEnum.DataProxyOut) {
                return row.count;
              } else {
                return (
                  auditData
                    .find(a => a.auditId === StatisticsTypeEnum.DataProxyIn)
                    ?.auditSet?.find(d => d.logTs === row.logTs)?.count || '-'
                );
              }
            },
          },
          {
            key: 'delay',
            header: '平均耗时',
            render: row => (row.name <= StatisticsTypeEnum.DataProxyOut ? '-' : row.delay || 0),
          },
          {
            key: 'count',
            header: '数据订阅量',
            render: row => (row.name <= StatisticsTypeEnum.DataProxyOut ? '-' : row.count),
          },
        ]}
      />
    </Form>
  );
};

export default Statistics;
