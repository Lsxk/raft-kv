package com.lsxk.raft.rpc;

import lombok.Getter;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Getter
public class LogEntry {
    private long index;

    private long term;
}
