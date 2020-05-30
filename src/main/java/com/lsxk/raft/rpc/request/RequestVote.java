package com.lsxk.raft.rpc.request;

import lombok.Builder;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Builder
public class RequestVote {

    /**
     * 候选者的任期号
     */
    private long term;

    /**
     * 候选者id
     */
    private String candidateId;

    /**
     * 候选者最新日志号
     */
    private long lastLogIndex;

    /**
     * 候选者最新日志的任期号
     */
    private long lastLogTerm;
}
