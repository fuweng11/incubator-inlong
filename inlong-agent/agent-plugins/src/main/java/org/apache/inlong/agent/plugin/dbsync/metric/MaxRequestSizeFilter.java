package org.apache.inlong.agent.plugin.dbsync.metric;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 功能描述：dbsync
 *
 * @Auther: nicobao
 * @Date: 2021/8/3 10:38
 * @Description:
 */
public class MaxRequestSizeFilter implements Filter {

    private final long maxSize;

    public MaxRequestSizeFilter(long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        long size = request.getContentLengthLong();

        if (size > maxSize || isChunked(request)) {
            // Size it's either unknown or too large
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad Request");
        } else {
            chain.doFilter(request, response);
        }
    }

    private static boolean isChunked(ServletRequest request) {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) request;
            String encoding = req.getHeader("Transfer-Encoding");
            return encoding != null && encoding.contains("chunked");
        } else {
            return false;
        }
    }

    @Override
    public void destroy() {
    }
}
