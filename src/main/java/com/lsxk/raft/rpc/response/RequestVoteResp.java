package com.lsxk.raft.rpc.response;

import lombok.Getter;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Getter
public class RequestVoteResp {
    /**
     * 应答节点的任期号
     */
    private long term;

    private boolean voteGranted;
}
