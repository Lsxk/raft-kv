package com.lsxk.raft.node;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Getter
@Setter
@ToString
public class NodeConfig {

    private int port;

    private String addr;

    private List<String> peerAdds;

    public List<String> getParents() {
        List<String> res = new ArrayList<>(peerAdds);
        res.remove(addr);
        return res;
    }
}
