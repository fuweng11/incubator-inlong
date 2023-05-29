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

#include "manager_auth.h"
#include "base64.h"
#include "http_client.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include <cstdlib>
#include <iostream>
#include <openssl/des.h>
#include <rapidjson/writer.h>
#include <sstream>
#include <sys/time.h>
#include <utility>
//using namespace zj;
namespace tauth {
#define SET_KS(_ks, _pos)                    \
    bzero(block_key, sizeof(block_key));     \
    memcpy(block_key, key.data() + _pos, 8); \
    DES_set_key(reinterpret_cast<DES_cblock *>(block_key), &_ks);


    using namespace rapidjson;
    std::string TAuthClient::constructAuthentication(std::string user,
                                                     std::string cmk,
                                                     std::string target,
                                                     std::string usHost,
                                                     std::string authPath) {
        UsSecret secret;
        secret.usHost = std::move(usHost);
        secret.authPath = std::move(authPath);
        secret.localHost = "127.0.0.1";
        secret.target = std::move(target);
        secret.user = std::move(user);
        secret.cmk = std::move(cmk);

        rapidjson::Document clientAuth;
        clientAuth.SetObject();
        rapidjson::Document::AllocatorType &allocator = clientAuth.GetAllocator();
        clientAuth.AddMember("principle", rapidjson::StringRef(secret.user.c_str()), allocator);
        clientAuth.AddMember("host", rapidjson::StringRef(secret.localHost.c_str()), allocator);
        clientAuth.AddMember("sequence", 1, allocator);
        clientAuth.AddMember("timestamp", getCurrentTime(), allocator);

        // Convert JSON document to string
        rapidjson::StringBuffer clientAuthenticator;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(clientAuthenticator);
        clientAuth.Accept(writer);

        //解码clientTicket
        std::string sessionKey = decryptClientTicket(secret);
        if (sessionKey.empty()) {
            return {};
        }
        std::string sessionKeyBase64 = base64::from_base64(sessionKey);
        std::string sclient_auth_encrypt;
        TripleDesEncrypt(clientAuthenticator.GetString(), sessionKeyBase64, &sclient_auth_encrypt);
        std::string client_auth_encoded = base64::to_base64(sclient_auth_encrypt);
        std::stringstream ssheaderSecret;
        ssheaderSecret << "tauth." << secret.st << "." << client_auth_encoded;
        return ssheaderSecret.str();
    }

    int TAuthClient::getSessionTicket(UsSecret &secret) {
        rapidjson::Document clientAuth;
        clientAuth.SetObject();
        rapidjson::Document::AllocatorType &allocator = clientAuth.GetAllocator();
        clientAuth.AddMember("user", rapidjson::StringRef(secret.user.c_str()), allocator);
        clientAuth.AddMember("localHost", rapidjson::StringRef(secret.localHost.c_str()), allocator);
        clientAuth.AddMember("target", rapidjson::StringRef(secret.target.c_str()), allocator);
        clientAuth.AddMember("lifetime", 7200000, allocator);
        clientAuth.AddMember("timestamp", getCurrentTime(), allocator);

        // Convert JSON document to string
        rapidjson::StringBuffer strbuf;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
        clientAuth.Accept(writer);

        auto base64 = base64::to_base64(strbuf.GetString());
        std::stringstream url;
        url << secret.authPath << "?ident=" << base64;

        std::string host = secret.usHost + url.str();

        tauth::HttpRequestPar httpRequestPar;
        httpRequestPar.url = host;
        httpRequestPar.type = "GET";
        tauth::HttpClient httpClient;
        std::string resBody;
        httpClient.requestUrl(resBody, httpRequestPar);

        rapidjson::Document root;
        if (root.Parse(resBody.c_str()).HasParseError()) {
            std::cout << "failed to parse user config file: " << resBody.c_str() << std::endl;
            return kFailed;
        }
        if (!root.HasMember("st")) {
            std::cout << "param st is not an object " << resBody.c_str() << std::endl;
            return kFailed;
        } else {
            const rapidjson::Value &objServiceTicket = root["st"];
            secret.st = objServiceTicket.GetString();
        }

        if (!root.HasMember("ct")) {
            std::cout << "param ct is not an object " << resBody.c_str() << std::endl;
            return kFailed;
        } else {
            const rapidjson::Value &objClientTicket = root["ct"];
            secret.ct = objClientTicket.GetString();
        }
        return kSuccess;
    }
    std::string TAuthClient::decryptClientTicket(UsSecret &secret) {
        if (getSessionTicket(secret) == kFailed) {
            return {};
        }
        // base64解码
        auto base64Cmk = base64::from_base64(secret.cmk);
        std::string cmkHex = HexDecode(base64Cmk);

        // base64解码
        std::string decryptCt;
        std::string decode_ct = base64::from_base64(secret.ct);
        TripleDesDecrypt(decode_ct, cmkHex, &decryptCt);

        //转换成json
        rapidjson::Document ClientTicketJson;
        if (ClientTicketJson.Parse(decryptCt.c_str()).HasParseError()) {
            std::cout << "failed to parse user config file: " << decryptCt << std::endl;
            return {};
        }
        const rapidjson::Value &objTimeStamp = ClientTicketJson["timestamp"];
        long timeStamp = objTimeStamp.GetInt64();

        const rapidjson::Value &objLifetime = ClientTicketJson["lifetime"];
        long lifetime = objLifetime.GetInt64();
        secret.expireTime = timeStamp + lifetime;

        if (ClientTicketJson.HasMember("sessionKey") && ClientTicketJson["sessionKey"].IsString()) {
            const rapidjson::Value &objSessionKey = ClientTicketJson["sessionKey"];
            return objSessionKey.GetString();
        }
        std::cout << "sessionKey is empty: " << std::endl;
        return {};
    }

    std::string TAuthClient::HexDecode(const std::string &hex) {
        std::string out;
        for (size_t i = 0; i < hex.length(); i += 2) {
            std::string byte = hex.substr(i, 2);
            char ch = (char) (int) strtol(byte.c_str(), NULL, 16);
            out.push_back(ch);
        }
        return out;
    }

    void TAuthClient::TripleDesDecrypt(const std::string &in, const std::string &key, std::string *out) {
        const auto block_size = sizeof(DES_cblock);
        if ((key.length() < 24) || (in.length() % block_size != 0)) {
            return;
        }
        DES_key_schedule ks1, ks2, ks3;
        unsigned char block_key[8];
        SET_KS(ks1, 0)
        SET_KS(ks2, 8)
        SET_KS(ks3, 16)
        int8_t pad;
        int block_num = in.size() / block_size;
        for (int i = 0; i < block_num; ++i) {
            DES_cblock input, output;
            bzero(output, sizeof(output));
            memcpy(input, in.data() + i * block_size, block_size);
            DES_ecb3_encrypt(&input, &output, &ks1, &ks2, &ks3, DES_DECRYPT);
            out->append(reinterpret_cast<char *>(output), sizeof(output));
            pad = output[sizeof(output) - 1];
        }
        *out = out->substr(0, out->size() - pad);
    }

    void TAuthClient::TripleDesEncrypt(const std::string &in, const std::string &key, std::string *out) {
        if (key.size() < 24) {
            return;
        }
        DES_key_schedule ks1, ks2, ks3;
        unsigned char block_key[8];
        SET_KS(ks1, 0)
        SET_KS(ks2, 8)
        SET_KS(ks3, 16)

        std::string in_copy(in);
        const auto block_size = sizeof(DES_cblock);
        const auto mod_size = in_copy.size() % block_size;
        in_copy.append(block_size - mod_size, block_size - mod_size);

        int block_num = in_copy.size() / block_size;
        for (int i = 0; i < block_num; ++i) {
            DES_cblock input, output;
            bzero(output, sizeof(output));
            memcpy(input, in_copy.data() + i * block_size, block_size);
            DES_ecb3_encrypt(&input, &output, &ks1, &ks2, &ks3, DES_ENCRYPT);
            out->append(reinterpret_cast<char *>(output), sizeof(output));
        }
    }

    uint64_t TAuthClient::getCurrentTime() {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return tv.tv_sec * 1000 + tv.tv_usec / 1000;
    }
    TAuthClient::TAuthClient() {
    }
}// namespace tauth