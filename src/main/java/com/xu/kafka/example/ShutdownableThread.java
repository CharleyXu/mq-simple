package com.xu.kafka.example;

import org.apache.kafka.common.internals.FatalExitError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by CharleyXu on 2020-07-30
 */
public abstract class ShutdownableThread extends Thread {

    public boolean isInterruptible() {
        return isInterruptible;
    }

    public String name() {
        return name;
    }

    private String name;

    private boolean isInterruptible = true;

    private Logger logger = LoggerFactory.getLogger(ShutdownableThread.class);

    private CountDownLatch shutdownInitiated = new CountDownLatch(1);

    private CountDownLatch shutdownComplete = new CountDownLatch(1);

    private volatile Boolean isStarted = false;

    public ShutdownableThread() {
        this.setDaemon(false);
    }

    public ShutdownableThread(String name, boolean isInterruptible) {
        this.name = name;
        this.isInterruptible = isInterruptible;
        super.setName(name);
    }

    @Override
    public void run() {
        isStarted = true;
        logger.info("Starting");
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (FatalExitError e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            logger.info("Stopped when FatalExitError");
            System.exit(e.statusCode());
        } catch (Throwable e) {
            if (isRunning()) {
                logger.error("Error due to", e);
            }
        } finally {
            shutdownComplete.countDown();
        }
        logger.info("Stopped");
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    private boolean isShutdownInitiated() {
        return shutdownInitiated.getCount() == 0;
    }

    private boolean isShutdownComplete() {
        return shutdownComplete.getCount() == 0;
    }

    private boolean isThreadFailed() {
        return isShutdownComplete() && !isShutdownInitiated();
    }

    private boolean initiateShutdown() {
        synchronized (this) {
            if (isRunning()) {
                logger.info("Shutting down");
                shutdownInitiated.countDown();
                if (isInterruptible) {
                    interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    private void awaitShutdown() throws InterruptedException {
        if (!isShutdownInitiated()) {
            throw new IllegalStateException("initiateShutdown() was not called before awaitShutdown()");
        } else {
            if (isStarted) {
                shutdownComplete.await();
                logger.info("Shutdown completed");
            }
        }
    }

    private void pause(Long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdownInitiated.await(timeout, unit)) {
            logger.trace("shutdownInitiated latch count reached zero. Shutdown called.");
        }
    }

    abstract void doWork();

    private boolean isRunning() {
        return !isShutdownInitiated();
    }

}
