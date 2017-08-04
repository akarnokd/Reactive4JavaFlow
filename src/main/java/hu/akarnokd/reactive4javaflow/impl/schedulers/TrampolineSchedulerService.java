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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.PlainQueue;
import hu.akarnokd.reactive4javaflow.impl.util.MpscLinkedArrayQueue;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class TrampolineSchedulerService implements SchedulerService {
    @Override
    public Worker worker() {
        return new TrampolineWorker();
    }

    static final class TrampolineWorker implements SchedulerService.Worker {

        final MpscLinkedArrayQueue<TrampolineTask> queue;

        long wip;
        static final VarHandle WIP;

        volatile boolean closed;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(TrampolineWorker.class, "wip", Long.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        TrampolineWorker() {
            queue = new MpscLinkedArrayQueue<>(32);
        }

        @Override
        public AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (!closed) {
                TrampolineTask tt = new TrampolineTask(task, unit.toMillis(delay));
                queue.offer(tt);
                drain();
                return tt;
            }
            return REJECTED;
        }

        @Override
        public void close() {
            closed = true;
            drain();
        }

        void drain() {
            if ((long)WIP.getAndAdd(this, 1) == 0) {
                PlainQueue<TrampolineTask> q = queue;
                do {
                    TrampolineTask tt = q.poll();
                    if (tt != null && !closed) {
                        tt.run();
                    }
                } while ((long)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }

        static final class TrampolineTask extends AtomicReference<Runnable> implements Runnable, AutoDisposable {

            final long sleepMillis;

            TrampolineTask(Runnable run, long sleepMillis) {
                this.sleepMillis = sleepMillis;
                setRelease(run);
            }

            @Override
            public void close() {
                setRelease(null);
            }

            @Override
            public void run() {
                Runnable r = getAcquire();
                if (r != null) {
                    setRelease(null);
                    try {
                        if (sleepMillis != 0L) {
                            Thread.sleep(sleepMillis);
                        }
                        r.run();
                    } catch (Throwable ex) {
                        FolyamPlugins.onError(ex);
                    }
                }
            }
        }
    }
}
