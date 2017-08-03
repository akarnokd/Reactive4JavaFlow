package hu.akarnokd.reactive4javaflow.impl.schedulers;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.util.concurrent.TimeUnit;

public enum ImmediateSchedulerService implements SchedulerService, SchedulerService.Worker {
    INSTANCE;

    static final AutoDisposable DONE = new ImmediatelyDone();

    @Override
    public AutoDisposable schedule(Runnable task) {
        try {
            task.run();
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
        }
        return DONE;
    }

    @Override
    public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("This SchedulerService doesn't support the timed operations!");
    }

    @Override
    public AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException("This SchedulerService doesn't support the timed operations!");
    }

    @Override
    public Worker worker() {
        return this;
    }

    @Override
    public long now(TimeUnit unit) {
        return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        // deliberately no-op
    }

    static final class ImmediatelyDone implements AutoDisposable {

        @Override
        public void close() {
            // deliberately no-op
        }

        @Override
        public String toString() {
            return "ImmediatelyDone";
        }
    }
}
