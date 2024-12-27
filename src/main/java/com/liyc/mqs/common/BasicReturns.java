package com.liyc.mqs.common;

import java.io.Serializable;

/**
 * 基础返回参数类
 *
 * @author Liyc
 * @date 2024/12/27 14:52
 **/

public class BasicReturns implements Serializable {
    // 表示一次请求/响应 的身份标识. 可以把请求和响应对上.
    protected String rid;
    // 这次通信使用的 channel 的身份标识.
    protected String channelId;
    // ok 表示这次API调用的结果 true：成功 false：失败
    protected boolean ok;

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }
}
