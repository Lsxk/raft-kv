package com.lsxk.raft.rpc.request;

import com.lsxk.raft.rpc.LogEntry;

import java.util.List;

import lombok.Builder;

/**
 * 功能描述：
 * 附件日志rpc,只由leader使用
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Builder
public class AppendEntries {
    private long term;

    private String leaderId;

    /**
     * 上一条日志号
     */
    private long prevLogIndex;

    /**
     * 上一条日志的任期号
     */
    private long prevLogTerm;

    private List<LogEntry> entries;

    /**
     * leader已经提交到哪里了
     */
    private long leaderCommit;
}
