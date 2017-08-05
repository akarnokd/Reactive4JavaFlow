/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.impl.schedulers;

import hu.akarnokd.reactive4javaflow.SchedulerService;
import hu.akarnokd.reactive4javaflow.impl.CleanerHelper;

import java.lang.invoke.*;
import java.lang.ref.Cleaner;
import java.util.concurrent.*;

public final class NewThreadSchedulerService implements SchedulerService, ThreadFactory {

    final String namePrefix;

    final int priority;

    final boolean daemon;

    long index;
    static final VarHandle INDEX;

    static {
        try {
            INDEX = MethodHandles.lookup().findVarHandle(NewThreadSchedulerService.class, "index", Long.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public NewThreadSchedulerService(String namePrefix, int priority, boolean daemon) {
        this.namePrefix = namePrefix;
        this.priority = priority;
        this.daemon = daemon;
    }

    @Override
    public Worker worker() {
        ScheduledExecutorService b = Executors.newScheduledThreadPool(1, this);
        ((ScheduledThreadPoolExecutor)b).setRemoveOnCancelPolicy(true);
        return new NewThreadWorker(b);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + ((long)INDEX.getAndAdd(this, 1) + 1));
        thread.setPriority(priority);
        thread.setDaemon(daemon);
        return thread;
    }

    static final class NewThreadWorker extends ScheduledExecutorServiceWorker {

        final Cleaner.Cleanable cleanable;

        static final class CleanupNewThreadWorker implements Runnable {
            final ScheduledExecutorService exec;

            CleanupNewThreadWorker(ScheduledExecutorService exec) {
                this.exec = exec;
            }

            @Override
            public void run() {
                exec.shutdownNow();
            }
        }

        public NewThreadWorker(ScheduledExecutorService exec) {
            super(exec);
            cleanable = CleanerHelper.register(this, new CleanupNewThreadWorker(exec));
        }

        @Override
        public void close() {
            cleanable.clean();
            super.close();
        }
    }
}
