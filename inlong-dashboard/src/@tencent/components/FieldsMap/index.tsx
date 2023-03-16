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
import React, { useCallback, useEffect, useState } from 'react';
import {
  Icon,
  Table,
  TableProps,
  Button,
  Row,
  Col,
  Input,
  Select,
  StatusTip,
} from '@tencent/tea-component';
import styles from './index.module.less';
import FieldsParse from '../FieldsParse';
import LongText from '../LongText';
import { AutoAdd, ConvertTip, Delete } from '../Icons';

const { injectable, selectable, draggable } = Table.addons;

export type FieldData = {
  id: number;
  sequence?: number;
  fieldName: string;
  fieldType: string;
  remark?: string;
}[];

export type selectedFieldsProps = {
  sourceField: FieldData[0];
  targetField: FieldData[0];
}[];

export interface ConnectTableProps extends Omit<TableProps<any>, 'columns'> {
  sourceFields: FieldData;
  targetFields: FieldData;
  onSelect: (data: selectedFieldsProps) => void; //get selected fields
  getTargetFields: (table?: string) => any; //get target fields by table name
  candoAutoAdd?: boolean; // default true
}

type actionsProps = {
  text: string;
  disabled?: boolean;
  icon?: React.ReactNode;
  onClick?: () => void;
}[];

const FieldsMap = ({
  sourceFields,
  targetFields: defaultTargetFields,
  onSelect,
  getTargetFields,
  candoAutoAdd = true,
  ...props
}: ConnectTableProps) => {
  const [fieldsParseVisible, setFieldParseVisible] = useState<boolean>(false);
  const [selectedFields, setSelectedFields] = useState<selectedFieldsProps>([]);
  const [targetFields, setTargetFields] = useState<FieldData>([]);
  const [status, setStatus] = useState<string>('');

  const actions: actionsProps = [
    {
      text: '批量解析字段',
      onClick: () => {
        setFieldParseVisible(true);
      },
      icon: <ConvertTip />,
    },
    {
      text: '自动添加',
      icon: <AutoAdd />,
      onClick: async () => {
        setStatus('loading');
        const newTargetFields = await getTargetFields();
        setStatus('found');
        setTargetFields(newTargetFields);
      },
      disabled: !candoAutoAdd,
    },
    {
      text: '全部删除',
      icon: <Delete />,
      onClick: () => {
        setTargetFields([]);
      },
    },
  ];

  const columns = (type: string): TableProps['columns'] => {
    return [
      {
        key: 'fieldName',
        header: '源表字段',
        width: 50,
        render: records =>
          type === 'edit' ? (
            <Input
              defaultValue={records.fieldName}
              onChange={val => {
                onChangeFieldValue('fieldName', records, val);
              }}
            />
          ) : (
            <LongText text={records.fieldName} style={{ minHeight: '30px', lineHeight: '30px' }} />
          ),
      },
      {
        key: 'fieldType',
        header: '类型',
        width: 50,
        render: records =>
          type === 'edit' ? (
            <Select
              size="full"
              appearance="button"
              defaultValue={records.fieldType}
              options={['int', 'long', 'float', 'double', 'string', 'date', 'timestamp'].map(
                item => ({
                  text: item,
                  value: item,
                }),
              )}
              onChange={val => {
                onChangeFieldValue('fieldType', records, val);
              }}
            />
          ) : (
            <LongText text={records.fieldType} style={{ minHeight: '30px', lineHeight: '30px' }} />
          ),
      },
      {
        key: 'remark',
        header: '备注',
        width: 50,
        render: records => <LongText text={records.remark} />,
      },
    ];
  };

  //listen target fields change
  const onChangeFieldValue = (type: 'fieldType' | 'fieldName', records, val) => {
    const copyTargetFields = targetFields;
    for (let targetField of copyTargetFields) {
      if (targetField.id === records.id) {
        targetField[type] = val;
      }
    }
    setTargetFields(copyTargetFields);
  };

  useEffect(() => {
    setTargetFields(defaultTargetFields);
  }, [defaultTargetFields]);

  useEffect(() => {
    const newSelectedFields = selectedFields.map(item => ({
      sourceField: item.sourceField,
      targetField: targetFields.filter(target => target.id == item.sourceField?.id)[0],
    }));
    setSelectedFields(newSelectedFields);
    onSelect(newSelectedFields);
  }, [targetFields]);

  return (
    <>
      <Row style={{ background: '#f2f2f2', margin: 0 }}>
        <Col span={12}>
          <div className={styles.leftCol}>
            <div className={styles.sourceTable}>
              <div className={styles.tableTitle}>来源字段</div>
              <Table
                {...props}
                recordKey="id"
                records={sourceFields}
                columns={columns('show')}
                addons={[
                  selectable({
                    value: selectedFields.map(item => item.sourceField.id.toString()),
                    onChange: (keys, context) => {
                      const valueMaps = keys.map(key => ({
                        sourceField: context.selectedRecords.filter(
                          item => item.id.toString() === key,
                        )[0],
                        targetField: targetFields.filter(item => item.id.toString() === key)[0],
                      }));
                      setSelectedFields(valueMaps);
                      onSelect(valueMaps);
                    },
                  }),
                ]}
              />
            </div>
            <div>
              <div style={{ height: '65px' }}></div>
              <div style={{ height: 'calc(100% - 65px)' }}>
                {sourceFields.map((item, i) => (
                  <div
                    style={{
                      height: `${100 / sourceFields.length}%`,
                      lineHeight: '40px',
                      visibility: selectedFields.map(item => item.sourceField.id).includes(i)
                        ? 'visible'
                        : 'hidden',
                    }}
                    key={i}
                  >
                    <span style={{ fontSize: '20px', color: '#CBCBCB' }}>&#10230;</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Col>

        <Col span={12}>
          <div className={styles.tableTitle}>
            <div>写入字段</div>
            <div className={styles.actions}>
              {actions.map((action, i) => (
                <span className={styles.action} key={i}>
                  <div className={styles.icon}>{action.icon}</div>
                  <Button type="link" onClick={action.onClick} disabled={action.disabled}>
                    {action.text}
                  </Button>
                </span>
              ))}
            </div>
          </div>
          <Table
            {...props}
            topTip={status === 'loading' && <StatusTip status="loading" />}
            recordKey="fieldName"
            columns={columns('edit')}
            records={targetFields}
            addons={[
              draggable({
                onDragEnd: (records, context) => {
                  //change target fields
                  setTargetFields(records);
                  //change selected fields
                  const newSelectedFields = selectedFields.map((selectedField, i) => ({
                    sourceField: selectedField.sourceField,
                    targetField: records[selectedField.sourceField?.id],
                  }));
                  setSelectedFields(newSelectedFields);
                  onSelect(newSelectedFields);
                },
              }),
              injectable({
                row: (props, context) => ({
                  className: selectedFields.some(
                    item => item.sourceField?.id === context.recordIndex,
                  )
                    ? 'is-selected'
                    : undefined,
                }),
              }),
            ]}
          />
        </Col>
      </Row>
      {fieldsParseVisible && (
        <FieldsParse
          visible={fieldsParseVisible}
          onOk={data => {
            setFieldParseVisible(false);
            setTargetFields(data);
          }}
          onClose={() => setFieldParseVisible(false)}
        />
      )}
    </>
  );
};

export default FieldsMap;
