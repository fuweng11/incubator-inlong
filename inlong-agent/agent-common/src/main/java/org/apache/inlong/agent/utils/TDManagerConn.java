package org.apache.inlong.agent.utils;

import com.tencent.tdw.security.authentication.v2.TauthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.agent.constant.DBSyncConfConstants;
import org.apache.inlong.agent.utils.JsonUtils.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;

//TODO:合并至HttpManager类
public class TDManagerConn {

	private static final Logger logger = LogManager.getLogger(TDManagerConn.class);
	public static final int CONNECTION_TIMEOUT = 100 * 1000;
	public static final int SOCKET_TIMEOUT = 100 * 1000;

	public static String cgi2TDManager(String url, HttpEntity entity, String token,
			String serviceName) {
		HttpParams myParams = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(myParams, CONNECTION_TIMEOUT);
		HttpConnectionParams.setSoTimeout(myParams, SOCKET_TIMEOUT);
		HttpClient httpclient = new DefaultHttpClient(myParams);
		HttpPost httpPost = getAuthHttpPost(url, token, serviceName);
		String returnStr = null;
		try {
			httpPost.setEntity(entity);
			HttpResponse response = httpclient.execute(httpPost);
			returnStr = EntityUtils.toString(response.getEntity());
			logger.debug("Now Post Message from {}, params: {}, return code: {}, return string: {}",
					url, EntityUtils.toString(entity), response.getStatusLine().getStatusCode(), returnStr);
			if (returnStr != null && !returnStr.isEmpty() && response.getStatusLine().getStatusCode() == 200) {
				//parse return str to JSONObject
				return returnStr;
			} else {
				logger.warn("response body is error Info or null from tdmanager");
				returnStr = null;
			}
		} catch (Throwable e) {
			logger.error("postdata to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
		} finally {
			try {
				httpPost.releaseConnection();
			} catch (Throwable e) {
				logger.error("postdata to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
			}
			try {
				httpclient.getConnectionManager().shutdown();
			} catch (Throwable e) {
				logger.error("postdata to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
			}
		}
		return returnStr;
	}

	private static HttpPost getAuthHttpPost(String url, String token, String serviceName) {
		HttpPost httpPost = new HttpPost(url);
		try {
			TauthClient tauthClient = new TauthClient(DBSyncConfConstants.AGENT_TAUTH_TEST_USER_NAME,
				DBSyncConfConstants.AGENT_TAUTH_TEST_USER_KEY);
			String encodedAuthentication = tauthClient.getAuthentication(serviceName);
			httpPost.addHeader(token, encodedAuthentication);
		} catch (SecureException e) {
			logger.error("get http fail with ex", e);
		}

		return httpPost;
	}

	private static HttpGet getAuthHttpGet(String url, String token, String serviceName) {
		HttpGet httpGet = new HttpGet(url);
		try {
			TauthClient tauthClient = new TauthClient(DBSyncConfConstants.AGENT_TAUTH_TEST_USER_NAME,
					DBSyncConfConstants.AGENT_TAUTH_TEST_USER_KEY);
			String encodedAuthentication = tauthClient.getAuthentication(serviceName);
			httpGet.addHeader(token, encodedAuthentication);
		} catch (SecureException e) {
			logger.error("get http fail with ex", e);
		}

		return httpGet;
	}

	public static String cgi2TDManager(String url, JSONObject params, String token,
			String serviceName) {
		StringEntity stringEntity = null;
		try {
			stringEntity = new StringEntity(params.toJSONString());
			stringEntity.setContentType(DBSyncConfConstants.AGENT_HTTP_APPLICATION_JSON);
		} catch (UnsupportedEncodingException e) {
			logger.error("error when get stringEntity", e);
		}

		return cgi2TDManager(url, stringEntity, token, serviceName);
	}

	public static String cgi2TDManagerForGet(String url, String token,
			String serviceName) {
		BasicHttpParams myParams = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(myParams, CONNECTION_TIMEOUT);
		HttpConnectionParams.setSoTimeout(myParams, SOCKET_TIMEOUT);
		HttpClient httpclient = new DefaultHttpClient(myParams);
		HttpGet httpGet = getAuthHttpGet(url, token, serviceName);
		String returnStr = null;
		String jsonRes = null;

		try {
			HttpResponse response = httpclient.execute(httpGet);
			returnStr = EntityUtils.toString(response.getEntity());
			logger.debug("Now Get Message from {}, params: {}, return code: {}, return string: {}",
					url, myParams, response.getStatusLine().getStatusCode(), returnStr);
			if (returnStr != null && !returnStr.isEmpty() && response.getStatusLine().getStatusCode() == 200) {
				//parse return str to JSONObject
				jsonRes = returnStr;
			} else {
				logger.debug("get response body is empty or null from tdmanager");
			}
		} catch (Throwable e) {
			logger.error("get data to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
		} finally {
			try {
				httpGet.releaseConnection();
			} catch (Throwable e) {
				logger.error("get data to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
			}
			try {
				httpclient.getConnectionManager().shutdown();
			} catch (Throwable e) {
				logger.error("get data to tdmanager ex {}", DBSyncUtils.getExceptionStack(e));
			}
		}
		return jsonRes;
	}
}
