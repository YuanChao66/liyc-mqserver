package com.liyc.mqs.common;

/**
 * 这个对象表示一个响应. 也是根据自定义应用层协议来的
 * 属性：1、类型 2、长度 3、正文
 *
 * @author Liyc
 * @date 2024/12/25 17:51
 **/

public class Response {
    private int type;
    private int length;
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
