package org.apache.inlong.agent.entites;

import com.google.gson.Gson;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class CommonResponse<T> {
    private static Gson gson = new Gson();
    private String errMsg;
    private boolean success;
    private T data;

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public static CommonResponse fromJson(String json, Class clazz) {
        Type objectType = type(CommonResponse.class, clazz);
        return gson.fromJson(json, objectType);
    }

    private static ParameterizedType type(final Class raw, final Type... args) {
        return new ParameterizedType() {
            public Type getRawType() {
                return raw;
            }
            public Type[] getActualTypeArguments() {
                return args;
            }
            public Type getOwnerType() {
                return null;
            }
        };
    }
}
