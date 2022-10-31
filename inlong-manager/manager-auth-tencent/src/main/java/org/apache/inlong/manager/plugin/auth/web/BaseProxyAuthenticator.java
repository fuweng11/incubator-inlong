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

package org.apache.inlong.manager.plugin.auth.web;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.UserTypeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.plugin.common.enums.AuthenticationType;
import org.apache.inlong.manager.plugin.common.pojo.user.StaffDTO;
import org.apache.inlong.manager.plugin.common.utils.SmallTools;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.user.UserRequest;
import org.apache.inlong.manager.service.tencentauth.SmartGateService;
import org.apache.inlong.manager.service.tencentauth.StaffBaseInfo;
import org.apache.inlong.manager.service.user.UserService;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.Authenticator;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;

/**
 * Base proxy authenticator
 */
@Slf4j
@Service
public abstract class BaseProxyAuthenticator implements Authenticator {

    private static final String DUMMY_PASSWORD = "******";
    private static final Integer DEFAULT_VALID_DAYS = 30 * 6;

    private final UserService userService;
    private final SmartGateService smartGateService;

    public BaseProxyAuthenticator(UserService userService, SmartGateService smartGateService) {
        this.userService = userService;
        this.smartGateService = smartGateService;
    }

    @Override
    public AuthenticationInfo authenticate(AuthenticationToken authenticationToken) throws AuthenticationException {
        String username = getUserName(authenticationToken);
        String proxyUser = getProxyUser(authenticationToken);
        if (StringUtils.isBlank(username)) {
            throw new AuthenticationException("username is empty");
        }

        try {
            UserInfo userInfo = createOrUpdateUser(username, proxyUser);
            return new SimpleAuthenticationInfo(userInfo, "", username);
        } catch (Exception e) {
            log.error("create or update login user fail: ", e);
            throw new AuthenticationException("auto register fail:" + username);
        }
    }

    private StaffDTO getStaffInfo(String username, String proxyUser) {
        StaffBaseInfo staffBaseInfo = smartGateService.getStaffByEnName(username);
        Preconditions.checkNotNull(staffBaseInfo, "staff not exist :" + username);

        String[] deptNames = staffBaseInfo.getDeptNameString().split("/");
        StaffDTO staff = new StaffDTO();
        staff.setUsername(username);
        staff.setDeptFullId(staffBaseInfo.getDeptIDString());
        staff.setDeptFullName(staffBaseInfo.getDeptNameString());
        staff.setDeptId(staffBaseInfo.getDepartmentID());
        staff.setDeptName(deptNames.length > 0 ? deptNames[deptNames.length - 1] : null);
        staff.setLastLoginTime(new Date());
        staff.setAuthenticationType(getAuthenticationType());
        staff.setProxyUser(proxyUser);
        staff.setBgId(getBgId(staff.getDeptFullId()));
        staff.setBgName(getBgName(staff.getDeptFullName()));
        staff.setBgEnName(SmallTools.getBgEnName(staff.getBgName()));
        return staff;
    }

    private String getBgName(String deptFullName) {
        if (deptFullName == null) {
            return null;
        }
        String[] deptNames = deptFullName.split("/");
        return deptNames.length > 0 ? deptNames[0] : null;
    }

    private String getBgId(String deptFullId) {
        if (deptFullId == null) {
            return null;
        }
        String[] deptIds = deptFullId.split(";");
        return deptIds.length > 2 ? deptIds[2] : null;
    }

    private UserInfo createOrUpdateUser(String username, String proxyUser) {
        StaffDTO staffInfo = getStaffInfo(username, proxyUser);
        UserInfo userInfo = userService.getByName(username);
        if (userInfo != null) {
            // update with the latest staff info
            userInfo.setExtParams(StaffDTO.convertToJson(staffInfo));
            UserRequest request = CommonBeanUtils.copyProperties(userInfo, UserRequest::new);
            request.setPassword(DUMMY_PASSWORD);
            try {
                userService.update(request, username);
                userInfo.setRoles(Collections.singleton(UserTypeEnum.name(userInfo.getAccountType())));
            } catch (BusinessException e) {
                if (e.getCode() == ErrorCodeEnum.CONFIG_EXPIRED.getCode()) {
                    // ignore concurrent update because very likely no data needs to update
                    log.warn("user info {} might be out of date", userInfo);
                }
            }
            return userInfo;
        }
        // or create with staff info
        UserRequest request = UserRequest.builder()
                .name(username)
                .password(DUMMY_PASSWORD)
                .validDays(DEFAULT_VALID_DAYS)
                .accountType(UserTypeEnum.OPERATOR.getCode())
                .extParams(StaffDTO.convertToJson(staffInfo))
                .build();
        int userId = userService.save(request, username);
        userInfo = CommonBeanUtils.copyProperties(request, UserInfo::new);
        userInfo.setId(userId);
        userInfo.setRoles(Collections.singleton(UserTypeEnum.name(userInfo.getAccountType())));
        return userInfo;
    }

    public String getProxyUser(AuthenticationToken authenticationToken) {
        if (!(authenticationToken instanceof ProxyUserAuthenticationToken)) {
            return null;
        }

        ProxyUserAuthenticationToken proxyUserAuthenticationToken = (ProxyUserAuthenticationToken) authenticationToken;
        if (allowProxy(proxyUserAuthenticationToken)) {
            return proxyUserAuthenticationToken.getProxyUser();
        }

        return null;
    }

    public abstract String getUserName(AuthenticationToken authenticationToken);

    public abstract AuthenticationType getAuthenticationType();

    private boolean allowProxy(ProxyUserAuthenticationToken token) {
        // inlong currently lacks the white list mechanism, disabled for now
        return false;
    }
}
