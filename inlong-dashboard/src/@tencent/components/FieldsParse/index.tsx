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

import React, { useState, useCallback } from 'react';
import {
  Modal,
  ModalProps,
  Button,
  Form,
  Input,
  Segment,
  Select,
  Table,
  Text,
  Icon,
  StatusTip,
} from '@tencent/tea-component';
import Editor from '@monaco-editor/react';

const { selectable, scrollable } = Table.addons;

const defaultJSON = `{
  "key": "value"
}`;

type ParsedData = {
  id: number;
  fieldName: string;
  fieldType: string;
  fieldComment?: string;
}[];

interface FieldsParseProps extends ModalProps {
  onOk: (data: ParsedData) => void;
}

const FieldsParse: React.FC<FieldsParseProps> = ({ onOk, onClose, ...rest }) => {
  const [parseType, setParseType] = useState('JSON');

  const [input, setInput] = useState(defaultJSON);
  const [inputErrMsg, setInputErrMsg] = useState('');

  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);

  const [parsedData, setParsedData] = useState<ParsedData>([]);

  const onParsedDataChange = (id: number, key: string, value: string) => {
    const newData = parsedData.map(item => {
      if (item.id === id) {
        return {
          ...item,
          [key]: value,
        };
      }
      return item;
    });
    setParsedData(newData);
  };

  const calcFieldType = useCallback((data: unknown): string => {
    if (typeof data === 'number') {
      return data % 1 === 0 ? 'int' : 'float';
    } else {
      return 'string';
    }
  }, []);

  const parseJSON = useCallback(
    (jsonStr: string): Promise<ParsedData> => {
      return new Promise((resolve, reject) => {
        try {
          const obj = JSON.parse(jsonStr);
          const arr = Object.keys(obj).map((fieldName, id) => ({
            id,
            fieldName,
            fieldType: calcFieldType(obj[fieldName]),
          }));
          resolve(arr);
        } catch (err) {
          reject(err);
        }
      });
    },
    [calcFieldType],
  );

  const parse = async () => {
    setInputErrMsg(input ? '' : '请输入内容');
    if (!input) return;
    try {
      if (parseType === 'JSON') {
        const result = await parseJSON(input);
        setParsedData(result);
      } else {
        console.log('SQL暂未支持');
      }
    } catch (err) {
      setInputErrMsg(err.message);
    }
  };

  const clear = () => {
    setInput('');
  };

  const handleOk = () => {
    const selectedParsedData = parsedData.filter(({ id }) => selectedKeys.includes(id.toString()));
    onOk(selectedParsedData);
  };

  return (
    <Modal caption="批量解析字段" onClose={onClose} {...rest}>
      <Modal.Body>
        <Form>
          <Form.Item label="添加方式">
            <Segment
              value={parseType}
              onChange={value => setParseType(value)}
              options={[
                { text: 'Json解析', value: 'JSON' },
                { text: 'SQL解析', value: 'SQL' },
              ]}
            />
          </Form.Item>
          <Form.Item
            label="编辑框"
            status={inputErrMsg ? 'error' : undefined}
            message={inputErrMsg}
          >
            <div
              style={{
                display: 'inline-block',
                outline: '1px solid #cfd5de',
                width: 'calc(100% - 2px)',
                marginLeft: 1,
              }}
            >
              <Editor
                height="150px"
                language={parseType.toLowerCase()}
                value={input}
                onChange={value => setInput(value)}
              />
            </div>
          </Form.Item>
          <Form.Item>
            <Button type="weak" onClick={parse}>
              解析
            </Button>
            <Button type="text" onClick={clear} style={{ marginLeft: 10 }}>
              清空
            </Button>
          </Form.Item>
          <Form.Item label="解析数据">
            <Table
              bordered
              compact
              records={parsedData}
              recordKey="id"
              columns={[
                {
                  key: 'fieldName',
                  header: '字段',
                  render: row => (
                    <Input
                      value={row.fieldName}
                      onChange={value => onParsedDataChange(row.id, 'fieldName', value)}
                    />
                  ),
                },
                {
                  key: 'fieldType',
                  header: '类型',
                  render: row => (
                    <Select
                      appearance="button"
                      options={[
                        'int',
                        'long',
                        'float',
                        'double',
                        'string',
                        'date',
                        'timestamp',
                      ].map(item => ({
                        text: item,
                        value: item,
                      }))}
                      value={row.fieldType}
                      onChange={value => onParsedDataChange(row.id, 'fieldType', value)}
                    />
                  ),
                },
                parseType === 'SQL' && {
                  key: 'fieldComment',
                  header: '备注',
                  render: row => (
                    <Input
                      value={row.fieldComment}
                      onChange={value => onParsedDataChange(row.id, 'fieldComment', value)}
                    />
                  ),
                },
              ].filter(Boolean)}
              addons={[
                selectable({
                  value: selectedKeys,
                  onChange: keys => setSelectedKeys(keys),
                  rowSelect: false,
                }),
                scrollable({
                  maxHeight: 215,
                }),
              ]}
              topTip={!parsedData.length && <StatusTip status="empty" />}
            />
          </Form.Item>
        </Form>
        <Text theme="primary" style={{ display: 'flex', alignItems: 'center', marginTop: 10 }}>
          <Icon type="infoblue" />
          <span style={{ marginLeft: 5 }}>解析代码后，选择解析字段覆盖表单中原有内容</span>
        </Text>
      </Modal.Body>

      <Modal.Footer>
        <Button type="primary" onClick={handleOk}>
          确定
        </Button>
        <Button type="weak" onClick={onClose}>
          取消
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default FieldsParse;
