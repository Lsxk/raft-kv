package com.lsxk.raft.enums;

import lombok.AllArgsConstructor;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@AllArgsConstructor
public enum NodeStatus {
    FOLLOWER(0),

    CANDIDATE(1),

    LEADER(2);

    private int status;
}
