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

#ifndef MANAGER_AUTH_HTTP_H
#define MANAGER_AUTH_HTTP_H
#include <string>
#include <vector>
namespace tauth {
    struct HttpRequestPar {
        std::string type;
        std::string url;
        int timeout;
        std::vector<std::string> header;
        std::string post_data;
    };

    enum HttpRetCode {
        kHttpSuccess = 0,
        kHttpInitFailed,
        kHttpExcuteFailed,
        kHttpGetInfoFailed,
        kHttpResEmpty
    };
    using namespace std;
    class HttpClient {
    public:
        HttpClient(){};
        int requestUrl(std::string &res, const HttpRequestPar request);
        static size_t getUrlResponse(void* buffer, size_t size, size_t count, void* response);
    private:
        std::string host;
    };
}// namespace tauth
#endif//MANAGER_AUTH_HTTP_H