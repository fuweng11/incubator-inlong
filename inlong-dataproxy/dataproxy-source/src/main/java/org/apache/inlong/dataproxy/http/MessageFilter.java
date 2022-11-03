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

package org.apache.inlong.dataproxy.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.dataproxy.consts.AttrConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class MessageFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(MessageFilter.class);

    private final int maxMsgLength;

    public MessageFilter(int maxMsgLength) {
        this.maxMsgLength = maxMsgLength;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        int code = StatusCode.SUCCESS;
        String message = "success";

        String pathInfo = req.getPathInfo();
        if (pathInfo.startsWith("/")) {
            pathInfo = pathInfo.substring(1);
        }
        if ("heartbeat".equals(pathInfo)) {
            resp.setCharacterEncoding(req.getCharacterEncoding());
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.flushBuffer();
            return;
        }

        String invalidKey = null;
        String groupId = req.getParameter(AttributeConstants.GROUP_ID);
        String streamId = req.getParameter(AttributeConstants.STREAM_ID);
        String dt = req.getParameter(AttributeConstants.DATA_TIME);
        String body = req.getParameter(AttrConstants.BODY);

        if (StringUtils.isEmpty(groupId)) {
            invalidKey = "groupId";
        } else if (StringUtils.isEmpty(streamId)) {
            invalidKey = "streamId";
        } else if (StringUtils.isEmpty(dt)) {
            invalidKey = "dt";
        } else if (StringUtils.isEmpty(body)) {
            invalidKey = "body";
        }

        try {
            if (invalidKey != null) {
                LOG.warn("Received bad request from client. " + invalidKey + " is empty.");
                code = StatusCode.ILLEGAL_ARGUMENT;
                message = "Bad request from client. " + invalidKey + " must not be empty.";
            } else if (body.length() > maxMsgLength) {
                LOG.warn("Received bad request from client. Body length is " + body.length());
                code = StatusCode.EXCEED_LEN;
                message = "Bad request from client. Body length is exceeding the limit:" + maxMsgLength;
            } else {
                chain.doFilter(request, response);
            }
        } catch (Throwable t) {
            code = StatusCode.SERVICE_ERR;
            if ((t instanceof ChannelException)) {
                message = "Channel error!";
            } else {
                message = "Service error!";
                LOG.error("Request error!", t);
            }
        }

        resp.setCharacterEncoding(req.getCharacterEncoding());
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().write(getResultContent(code, message));
        resp.flushBuffer();
    }

    @Override
    public void destroy() {
    }

    private String getResultContent(int code, String message) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"code\":\"");
        builder.append(code);
        builder.append("\",\"msg\":\"");
        builder.append(message);
        builder.append("\"}");

        return builder.toString();
    }

}
