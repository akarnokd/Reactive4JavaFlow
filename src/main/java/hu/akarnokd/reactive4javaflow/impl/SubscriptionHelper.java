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

import hu.akarnokd.reactive4javaflow.FolyamSubscriber;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.lang.invoke.VarHandle;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

public enum SubscriptionHelper implements Flow.Subscription {
    CANCELLED;

    public static boolean replace(AtomicReference<Flow.Subscription> field, Flow.Subscription d) {
        for (;;) {
            Flow.Subscription a = field.get();
            if (a == CANCELLED) {
                if (d != null) {
                    d.cancel();
                }
                return false;
            }
            if (field.compareAndSet(a, d)) {
                return true;
            }
        }
    }

    public static boolean replace(Object target, VarHandle upstream, Flow.Subscription d) {
        for (;;) {
            Object a = upstream.getAcquire(target);
            if (a == CANCELLED) {
                if (d != null) {
                    d.cancel();
                }
                return false;
            }
            if (upstream.compareAndSet(target, a, d)) {
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

    public static boolean cancel(Object target, VarHandle upstream) {
        Flow.Subscription a = (Flow.Subscription)upstream.getAcquire(target);
        if (a != CANCELLED) {
            a = (Flow.Subscription)upstream.getAndSet(target, CANCELLED);
            if (a != CANCELLED) {
                if (a != null) {
                    a.cancel();
                }
                return true;
            }
        }
        return false;
    }

    public static boolean isCancelled(Object target, VarHandle upstream) {
        return upstream.getAcquire(target) == CANCELLED;
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
            if (requested.compareAndSet(target, a, b)) {
                return a;
            }
        }
    }

    public static long addRequested(AtomicLong requested, long n) {
        for (;;) {
            long a = (long)requested.getAcquire();
            if (a == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long b = addCap(a, n);
            if (requested.compareAndSet(a, b)) {
                return a;
            }
        }
    }

    public static long addRequestedCancellable(AtomicLong requested, long n) {
        for (;;) {
            long a = (long)requested.getAcquire();
            if (a == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            if (a == Long.MIN_VALUE) {
                return Long.MIN_VALUE;
            }
            long b = addCap(a, n);
            if (requested.compareAndSet(a, b)) {
                return a;
            }
        }
    }

    public static boolean deferredReplace(Object target, VarHandle upstream, VarHandle requested, Flow.Subscription s) {
        if (replace(target, upstream, s)) {
            long n = (long)requested.getAndSet(target, 0L);
            if (n != 0L) {
                s.request(n);
            }
            return true;
        }
        return false;
    }

    public static void deferredRequest(Object target, VarHandle upstream, VarHandle requested, long n) {
        Flow.Subscription a = (Flow.Subscription)upstream.getAcquire(target);
        if (a != null) {
            a.request(n);
        } else {
            addRequested(target, requested, n);
            a = (Flow.Subscription)upstream.getAcquire(target);
            if (a != null) {
                n = (long)requested.getAndSet(target, 0L);
                if (n != 0L) {
                    a.request(n);
                }
            }
        }
    }

    public static <T> void postComplete(FolyamSubscriber<? super T> actual,
                                        Queue<T> queue, Object target, VarHandle requested,
                                        VarHandle cancelled) {
        for (;;) {
            long r = (long)requested.getAcquire(target);
            if ((r & Long.MIN_VALUE) != 0) {
                break;
            }
            if (requested.compareAndSet(target, r, r | Long.MIN_VALUE)) {
                if (r != 0L) {
                    postCompleteDrain(actual, queue, target, requested, cancelled, r | Long.MIN_VALUE);
                }
                break;
            }
        }
    }

    static <T> void postCompleteDrain(FolyamSubscriber<? super T> actual,
                                               Queue<T> queue, Object target, VarHandle requested,
                                               VarHandle cancelled, long n) {
        long e = Long.MIN_VALUE;
        for (;;) {
            while (e != n) {
                if ((boolean)cancelled.getAcquire(target)) {
                    queue.clear();
                    return;
                }

                T v = queue.poll();
                if (v == null) {
                    actual.onComplete();
                    return;
                }

                actual.onNext(v);
                e++;
            }

            if ((boolean)cancelled.getAcquire(target)) {
                queue.clear();
                return;
            }

            if (queue.isEmpty()) {
                actual.onComplete();
                return;
            }

            n = (long)requested.getAcquire(target);
            if (n == e) {
                n &= Long.MAX_VALUE;
                n = (long)requested.getAndAdd(target, -n) - n;
                if (n == Long.MIN_VALUE) {
                    break;
                }
                e = Long.MIN_VALUE;
            }
        }
    }

    public static <T> boolean postCompleteRequest(FolyamSubscriber<? super T> actual,
                                      Queue<T> queue, Object target, VarHandle requested,
                                      VarHandle cancelled, long n) {
        for (;;) {
            long r = (long)requested.getAcquire(target);
            long m = r & Long.MIN_VALUE;
            long a = r & Long.MAX_VALUE;
            long b = a + n;
            if (b < 0L) {
                b = Long.MAX_VALUE;
            }
            if (requested.compareAndSet(target, r, b | m)) {
                if (m != 0L) {
                    if (a == 0L) {
                        postCompleteDrain(actual, queue, target, requested, cancelled, b | m);
                    }
                    return false;
                }
                return true;
            }
        }
    }

    public static <T> void postComplete(ConditionalSubscriber<? super T> actual,
                                        Queue<T> queue, Object target, VarHandle requested,
                                        VarHandle cancelled) {
        for (;;) {
            long r = (long)requested.getAcquire(target);
            if ((r & Long.MIN_VALUE) != 0) {
                break;
            }
            if (requested.compareAndSet(target, r, r | Long.MIN_VALUE)) {
                if (r != 0L) {
                    postCompleteDrain(actual, queue, target, requested, cancelled, r | Long.MIN_VALUE);
                }
                break;
            }
        }
    }

    static <T> void postCompleteDrain(ConditionalSubscriber<? super T> actual,
                                             Queue<T> queue, Object target, VarHandle requested,
                                             VarHandle cancelled, long n) {
        long e = Long.MIN_VALUE;
        for (;;) {
            while (e != n) {
                if ((boolean)cancelled.getAcquire(target)) {
                    queue.clear();
                    return;
                }

                T v = queue.poll();
                if (v == null) {
                    actual.onComplete();
                    return;
                }

                if (actual.tryOnNext(v)) {
                    e++;
                }
            }

            if ((boolean)cancelled.getAcquire(target)) {
                queue.clear();
                return;
            }

            if (queue.isEmpty()) {
                actual.onComplete();
                return;
            }

            n = (long)requested.getAcquire(target);
            if (n == e) {
                n &= Long.MAX_VALUE;
                n = (long)requested.getAndAdd(target, -n) - n;
                if (n == Long.MIN_VALUE) {
                    break;
                }
                e = Long.MIN_VALUE;
            }
        }
    }

    public static <T> boolean postCompleteRequest(ConditionalSubscriber<? super T> actual,
                                           Queue<T> queue, Object target, VarHandle requested,
                                           VarHandle cancelled, long n) {
        for (;;) {
            long r = (long)requested.getAcquire(target);
            long m = r & Long.MIN_VALUE;
            long a = r & Long.MAX_VALUE;
            long b = a + n;
            if (b < 0L) {
                b = Long.MAX_VALUE;
            }
            if (requested.compareAndSet(target, r, b | m)) {
                if (m != 0L) {
                    if (a == 0L) {
                        postCompleteDrain(actual, queue, target, requested, cancelled, b | m);
                    }
                    return false;
                }
                return true;
            }
        }
    }

    public static long produced(Object target, VarHandle requested, long n) {
        for (;;) {
            long r = (long)requested.getAcquire(target);
            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long u = Math.max(0L, r - n);
            if (requested.compareAndSet(target, r, u)) {
                return u;
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
