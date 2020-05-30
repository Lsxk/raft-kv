package com.lsxk.raft.rpc.response;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Builder
@Getter
@Setter
public class AppendEntriesResp {
    private long term;

    private boolean success;
}
