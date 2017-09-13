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
package hu.akarnokd.reactive4javaflow.impl.util;

import hu.akarnokd.reactive4javaflow.impl.VH;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SpscCountDownLatch {

    static {
        // avoid lost park/unpark due to class initialization
        Class<?> ls = LockSupport.class;
    }

    Object waiter;
    static final VarHandle WAITER = VH.find(MethodHandles.lookup(), SpscCountDownLatch.class, "waiter", Object.class);

    public final void countDown() {
        Object w = WAITER.getAndSet(this, this);
        if (w != null && w != this) {
            LockSupport.unpark((Thread)w);
        }
    }

    public final void await() throws InterruptedException {
        if (WAITER.compareAndSet(this, null, Thread.currentThread())) {
            for (;;) {
                LockSupport.park();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (WAITER.getAcquire(this) == this) {
                    return;
                }
            }
        }
    }

    public final boolean await(long time, TimeUnit unit) throws InterruptedException {
        if (WAITER.compareAndSet(this, null, Thread.currentThread())) {
            long deadline = System.currentTimeMillis() + unit.toMillis(time);
            for (;;) {
                LockSupport.parkUntil(deadline);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (WAITER.getAcquire(this) == this) {
                    return true;
                }
                if (System.currentTimeMillis() >= deadline) {
                    return false;
                }
            }
        }
        return true;
    }

    public final long getCount() {
        return WAITER.getAcquire(this) == this ? 0 : 1;
    }
}
