package com.lsxk.raft.common;

import java.util.Timer;
import java.util.TimerTask;

/**
 * 功能描述：
 *
 * @version 1.0.0
 * @since 2020-05-30
 */
public class ElectionTimer {

    private Timer timer = new Timer("election-timeout");

    private TimerTask task;

    private long delay;

    public ElectionTimer(TimerTask task, long delay) {
        this.task = task;
        this.delay = delay;
    }

    public void start() {
        timer.schedule(task, delay);
    }

    public void reset() {
        stop();
        start();
    }

    public void stop() {
        timer.cancel();
    }
}
