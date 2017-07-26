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
package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.lang.invoke.VarHandle;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

public enum SubscriptionHelper implements Flow.Subscription {
    CANCELLED;

    public static boolean replace(AtomicReference<Flow.Subscription> field, Flow.Subscription d) {
        for (;;) {
            Flow.Subscription a = field.get();
            if (a == CANCELLED) {
                return false;
            }
            if (field.compareAndSet(a, d)) {
                return true;
            }
        }
    }

    public static boolean cancel(AtomicReference<Flow.Subscription> field) {
        Flow.Subscription a = field.getAcquire();
        if (a != CANCELLED) {
            a = field.getAndSet(CANCELLED);
            if (a != CANCELLED) {
                if (a != null) {
                    a.cancel();
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Adds two long values and caps the sum at Long.MAX_VALUE.
     * @param a the first value
     * @param b the second value
     * @return the sum capped at Long.MAX_VALUE
     */
    public static long addCap(long a, long b) {
        long u = a + b;
        if (u < 0L) {
            return Long.MAX_VALUE;
        }
        return u;
    }

    /**
     * Multiplies two long values and caps the product at Long.MAX_VALUE.
     * @param a the first value
     * @param b the second value
     * @return the product capped at Long.MAX_VALUE
     */
    public static long multiplyCap(long a, long b) {
        long u = a * b;
        if (((a | b) >>> 31) != 0) {
            if (u / a != b) {
                return Long.MAX_VALUE;
            }
        }
        return u;
    }

    public static long addRequested(Object target, VarHandle requested, long n) {
        for (;;) {
            long a = (long)requested.getAcquire(target);
            if (a == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long b = addCap(a, n);
            if (requested.compareAndSet(a, b)) {
                return a;
            }
        }
    }

    @Override
    public void cancel() {
        // deliberately no-op
    }

    @Override
    public void request(long n) {
        // deliberately no-op
    }
}
