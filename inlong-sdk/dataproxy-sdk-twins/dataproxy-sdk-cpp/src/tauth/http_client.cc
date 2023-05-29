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

#include "http_client.h"
#include "curl/curl.h"
#include <iostream>
namespace tauth{
    int HttpClient::requestUrl(std::string &res, const tauth::HttpRequestPar request) {
        CURL *curl = NULL;
        struct curl_slist *list = NULL;

        curl_global_init(CURL_GLOBAL_ALL);

        curl = curl_easy_init();
        if (!curl) {
            return tauth::kHttpInitFailed;
        }

        // http header
        list = curl_slist_append(list, "Content-Type: application/x-www-form-urlencoded");
        for (auto header: request.header) {
            list = curl_slist_append(list, header.c_str());
        }
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);

        std::cout<<"requestUrl url:"<<request.url.c_str()<<std::endl;
        //set url
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, request.type.c_str());
        curl_easy_setopt(curl, CURLOPT_URL,request.url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.post_data.c_str());
        //register callback and get res
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &getUrlResponse);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);


        //execute curl request
        CURLcode ret = curl_easy_perform(curl);

        if (ret != 0) {
            if (curl) curl_easy_cleanup(curl);
            curl_global_cleanup();

            return tauth::kHttpExcuteFailed;
        }

        int32_t code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        if (code != 200) {
            if (curl) curl_easy_cleanup(curl);
            curl_global_cleanup();

            return tauth::kHttpGetInfoFailed;
        }

        if (res.empty()) {
            if (curl) curl_easy_cleanup(curl);
            curl_global_cleanup();
            return tauth::kHttpResEmpty;
        }

        //clean work
        curl_easy_cleanup(curl);
        curl_global_cleanup();

        return tauth::kHttpSuccess;
    }
    size_t HttpClient::getUrlResponse(void* buffer, size_t size, size_t count, void* response)
    {
        std::string* str = (std::string*)response;
        (*str).append((char*)buffer, size * count);
        return size * count;
    }
}
