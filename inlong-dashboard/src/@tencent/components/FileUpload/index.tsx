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
import React, { useRef, useState } from 'react';
import { Button, Form, Upload, UploadProps } from '@tencent/tea-component';
import { ProgressInfo } from '@tencent/tea-component/lib/upload/uploadFile';

export interface FileUploadProps extends Omit<UploadProps, 'children'> {
  message: string; //form check message
  onSuccess?: (result: object | string) => void; //upload success callback
  onError?: () => void; //upload error callback
}

const FileUpload = ({ message, action, accept, maxSize, onSuccess, onError }: FileUploadProps) => {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState(null);
  const [percent, setPercent] = useState(null);

  const xhrRef = useRef(null);

  const handleStart = (file: File, { xhr }) => {
    setFile(file);
    setStatus('validating');
    xhrRef.current = xhr;
  };

  const handleProgress = ({ percent }: ProgressInfo) => {
    setPercent(percent);
  };

  const handleSuccess = (result: object | string) => {
    setStatus('success');
    if (onSuccess) {
      onSuccess(result);
    }
  };

  const handleError = () => {
    setStatus('error');
    handleAbort();
    if (onError) {
      onError();
    }
  };

  const beforeUpload = (file: File, fileList: File[], isAccepted: boolean) => {
    if (!isAccepted) {
      setStatus('error');
    }
    return isAccepted;
  };

  const handleAbort = () => {
    if (xhrRef.current) {
      xhrRef.current.abort();
    }
    setFile(null);
    setStatus(null);
    setPercent(null);
  };
  return (
    <Form.Control status={status} message={message}>
      <Upload
        action={action}
        accept={accept}
        maxSize={maxSize}
        onStart={handleStart}
        onProgress={handleProgress}
        onSuccess={handleSuccess}
        onError={handleError}
        beforeUpload={beforeUpload}
      >
        {({ open }) => (
          <Upload.File
            filename={file && file.name}
            percent={percent}
            button={
              status === 'validating' ? (
                <Button onClick={handleAbort}>取消上传</Button>
              ) : (
                <>
                  <Button onClick={open}>{status ? '重新上传' : '选择文件'}</Button>
                  {status && (
                    <Button type="link" style={{ marginLeft: 8 }} onClick={handleAbort}>
                      删除
                    </Button>
                  )}
                </>
              )
            }
          />
        )}
      </Upload>
    </Form.Control>
  );
};

export default FileUpload;
