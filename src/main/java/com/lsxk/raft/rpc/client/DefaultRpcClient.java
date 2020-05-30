package com.lsxk.raft.rpc.client;

import com.lsxk.raft.rpc.Request;
import com.lsxk.raft.rpc.Response;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
public class DefaultRpcClient {

    private final static com.alipay.remoting.rpc.RpcClient CLIENT = new com.alipay.remoting.rpc.RpcClient();

    static {
        CLIENT.init();
    }
    
    public Response send(Request request, int timeout) {
        Response response = null;
        try {
            response = (Response) CLIENT.invokeSync(request.getUrl(), request, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    public Response send(Request request) {
        return send(request, 5000);
    }
}
