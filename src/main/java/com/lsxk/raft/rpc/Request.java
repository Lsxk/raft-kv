package com.lsxk.raft.rpc;

import com.lsxk.raft.enums.RpcType;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Getter
@Setter
@Builder
public class Request<T> implements Serializable {
    private RpcType type;

    private T obj;

    private String url;
}
