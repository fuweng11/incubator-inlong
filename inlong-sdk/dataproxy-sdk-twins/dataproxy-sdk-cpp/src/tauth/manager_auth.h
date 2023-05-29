/**
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

#ifndef MANAGER_AUTH_MANAGER_AUTH_H
#define MANAGER_AUTH_MANAGER_AUTH_H

#include <memory>
#include <string>
#include <utility>

namespace tauth {
    struct UsSecret {
        std::string st;        // smk 加密的 serviceTicket
        std::string ct;        // cmk 加密的 clientTicket
        std::string sessionKey;// 使用cmk对ct进行解密和base64 decode得到的字符串

        int64_t expireTime;   // timestamp + lifetime 过期时间  单位：s
        std::string user;     //  note: 用户
        std::string cmk;      // 认证中心cmk
        std::string localHost;// 源ip 可以省略
        std::string target;   //  目标服务 / idex-openapi || Common-Scheduler

        std::string usHost;  // 认证中心地址
        std::string authPath;// 认证中心路径
    };
    enum RetCode {
        kSuccess = 0,
        kFailed
    };

    class TAuthClient {
    private:
        int getSessionTicket(UsSecret &secret);
        uint64_t getCurrentTime();
        static std::string HexDecode(const std::string &hex);
        std::string decryptClientTicket(UsSecret &secret);
        void TripleDesDecrypt(const std::string &in, const std::string &key, std::string *out);
        void TripleDesEncrypt(const std::string &in, const std::string &key, std::string *out);

    public:
        TAuthClient();
        std::string constructAuthentication(std::string user,
                                            std::string cmk,
                                            std::string target = "inlong_manager",
                                            std::string usHost = "http://auth.tdw.oa.com",
                                            std::string authPath = "/api/auth/st2");
    };
#endif//MANAGER_AUTH_MANAGER_AUTH_H
}