/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "inlong_api.h"
#include "../core/api_imp.h"
namespace inlong {

InLongApi::InLongApi() { api_impl_ = std::make_shared<InLongApiImp>(); };
InLongApi::~InLongApi() { api_impl_->CloseApi(10); }

int32_t InLongApi::InitApi(const char *config_path) {
  return api_impl_->InitApi(config_path);
}

int32_t InLongApi::Send(const char *business_id, const char *table_id,
                        const char *msg, int32_t msg_len,
                        UserCallBack call_back) {
  return api_impl_->Send(business_id, table_id, msg, msg_len, call_back);
}

int32_t InLongApi::CloseApi(int32_t max_waitms) {
  return api_impl_->CloseApi(max_waitms);
}
int32_t InLongApi::AddBid(const std::vector<std::string> &groupids) {
  return api_impl_->AddBid(groupids);
}
} // namespace inlong