package com.lsxk.raft.node;

import com.lsxk.raft.enums.NodeStatus;
import com.lsxk.raft.enums.RpcType;
import com.lsxk.raft.rpc.LogEntry;
import com.lsxk.raft.rpc.Request;
import com.lsxk.raft.rpc.Response;
import com.lsxk.raft.rpc.client.DefaultRpcClient;
import com.lsxk.raft.rpc.request.AppendEntries;
import com.lsxk.raft.rpc.request.RequestVote;
import com.lsxk.raft.rpc.response.AppendEntriesResp;
import com.lsxk.raft.rpc.response.RequestVoteResp;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import lombok.Getter;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
@Getter
public class RaftNode {

    private NodeStatus nodeStatus = NodeStatus.FOLLOWER;

    /**
     * 所有服务器上持久存在的
     */
    private volatile long currentTerm;

    private String votedFor;

    private LinkedList<LogEntry> log;

    /**
     * 所有服务器上经常变的
     */
    private long commitIndex;

    private long lastApplied;

    /**
     * 在领导人里经常改变的 （选举后重新初始化）
     */
    private Map<String, Long> nextIndex;

    private Map<String, Long> matchIndex;

    // 自定义
    private long heartBeatInterval = 1000;

    private NodeConfig config;

    private DefaultRpcClient rpcClient;

    private Random random = new Random();

    // 上次接受心跳时间
    private long lastBeBeat;

    private void updateTerm(long newTerm) {
        currentTerm = newTerm;
        votedFor = null;
    }

    class HeartBeatTask implements Runnable {

        @Override
        public void run() {
            if (nodeStatus != NodeStatus.LEADER) {
                return;
            }
            config.getParents().forEach(s -> CompletableFuture.runAsync(() -> {
                AppendEntries entry = AppendEntries.builder()
                    .leaderId(config.getAddr())
                    .leaderCommit(commitIndex)
                    .term(currentTerm)
                    .build();
                Request<Object> request = Request.builder().url(s).type(RpcType.APPEND_ENTRIES).obj(entry).build();
                Response resp = rpcClient.send(request);
                long term = ((AppendEntriesResp) resp.getResult()).getTerm();
                if (term > currentTerm) {
                    updateTerm(term);

                    nodeStatus = NodeStatus.FOLLOWER;
                    nextIndex.clear();
                    matchIndex.clear();
                }
            }));
        }
    }

    // 此任务执行周期需随机
    class ElectionTask implements Runnable {

        @Override
        public void run() {
            if (nodeStatus == NodeStatus.LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            // TODO StringUtil和votedFor的清理，如果超时了，并且还没投过票，就变成候选者
            long timeout = random.nextInt(200);
            if ((current - lastBeBeat > timeout) && "".equals(votedFor)) {
                nodeStatus = NodeStatus.CANDIDATE;

                updateTerm(++currentTerm);
                votedFor = config.getAddr();
                List<String> parents = config.getParents();
                AtomicInteger success = new AtomicInteger(0);
                CountDownLatch latch = new CountDownLatch(parents.size());

                parents.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        // 发送投票选举
                        CompletableFuture.runAsync(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    LogEntry lastLog = log.getLast();
                                    RequestVote requestVote = RequestVote.builder()
                                        .candidateId(config.getAddr())
                                        .lastLogIndex(lastLog.getIndex())
                                        .lastLogTerm(lastLog.getTerm())
                                        .term(currentTerm)
                                        .build();
                                    Request<Object> req = Request.builder()
                                        .type(RpcType.REQUEST_VOTE)
                                        .url(s)
                                        .obj(requestVote)
                                        .build();
                                    Response res = rpcClient.send(req);
                                    RequestVoteResp result = (RequestVoteResp) res.getResult();
                                    if (result.getTerm() > currentTerm) {
                                        updateTerm(result.getTerm());
                                    }
                                    if (result.isVoteGranted()) {
                                        success.getAndIncrement();
                                    }
                                } catch (Exception e) {

                                } finally {
                                    latch.countDown();
                                }

                            }
                        });
                    }
                });

                try {
                    latch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (nodeStatus == NodeStatus.FOLLOWER) {
                    return;
                }
                if (success.get() > config.getPeerAdds().size() / 2) {
                    nodeStatus = NodeStatus.LEADER;
                    votedFor = "";

                    // TODO leader do something
                } else {
                    votedFor = "";
                }

            }

        }
    }

}

