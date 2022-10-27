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

package org.apache.inlong.manager.plugin.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.util.Preconditions;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * General small tools
 */
public class SmallTools {

    private static final Pattern BG_EN_NAME_PATTERN = Pattern.compile("^[a-z0-9]+");
    private static final Pattern LOWER_NUMBER_PATTERN = Pattern.compile("^(?![0-9]+$)[a-z_][a-z0-9_]{1,100}$");
    private static final char[] hexDigits = {
            '0', '1', '2', '3', '4',
            '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * Get BG short name from full name
     */
    public static String getBgEnName(String bgFullName) {
        if (bgFullName == null || bgFullName.isEmpty()) {
            return bgFullName;
        }
        Matcher matcher = BG_EN_NAME_PATTERN.matcher(bgFullName.toLowerCase());
        Preconditions.checkTrue(matcher.find(), "cannot get the bg en name :" + bgFullName);
        return matcher.group();
    }

    /**
     * Check IP format
     */
    public static boolean ipCheck(String ip) {
        if (ip != null && !ip.isEmpty()) {
            // ip regex matching
            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
            return ip.matches(regex);
        }
        return false;
    }

    /**
     * Check is lower case or num
     */
    public static boolean isLowerOrNum(String str) {
        if (StringUtils.isNotBlank(str)) {
            return LOWER_NUMBER_PATTERN.matcher(str).matches();
        }
        return false;
    }

    /**
     * Get MD5
     */
    public static String getMD5String(String source) throws NoSuchAlgorithmException {
        String retString = null;
        if (source == null) {
            return retString;
        }

        StringBuilder sb = new StringBuilder();
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(source.getBytes(), 0, source.length());
        byte[] retBytes = md.digest();
        for (byte b : retBytes) {
            sb.append(hexDigits[(b >> 4) & 0x0f]);
            sb.append(hexDigits[b & 0x0f]);
        }

        retString = sb.toString();
        return retString;
    }

}
