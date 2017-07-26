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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionArbiter extends AtomicInteger implements Flow.Subscription {

    Flow.Subscription currentSubscription;

    long requested;

    Flow.Subscription missedSubscription;

    long missedRequested;
    static final VarHandle MISSED_REQUESTED;

    long missedProduced;

    boolean cancelled;
    static final VarHandle CANCELLED;

    static {
        try {
            MISSED_REQUESTED = MethodHandles.lookup().findVarHandle(SubscriptionArbiter.class, "missedRequested", Long.TYPE);
            CANCELLED = MethodHandles.lookup().findVarHandle(SubscriptionArbiter.class, "cancelled", Boolean.TYPE);
        } catch (Throwable ex) {
            throw new InternalError(ex);
        }
    }

    public final void arbiterReplace(Flow.Subscription s) {
        if (getAcquire() == 0 && compareAndSet(0, 1)) {
            long r = 0L;
            if ((boolean)CANCELLED.getAcquire(this)) {
                s.cancel();
            } else {
                r = requested;
                currentSubscription = s;
            }
            if (decrementAndGet() != 0) {
                arbiterDrainLoop();
            }
            if (r != 0L) {
                s.request(r);
            }
        } else {
            missedSubscription = s;
            if (getAndIncrement() != 0) {
                return;
            }
            arbiterDrainLoop();
        }
    }

    public final void arbiterProduced(long n) {
        if (getAcquire() == 0 && compareAndSet(0, 1)) {
            long r = requested;
            if (r != Long.MAX_VALUE) {
                long u = r - n;
                if (u < 0L) {
                    u = 0L;
                }
                requested = u;
            }
            if (decrementAndGet() == 0) {
                return;
            }
        } else {
            missedProduced = n;
            if (getAndIncrement() != 0) {
                return;
            }
        }
        arbiterDrainLoop();
    }

    @Override
    public final void request(long n) {
        if (getAcquire() == 0 && compareAndSet(0, 1)) {
            long r = requested;
            long u = r + n;
            if (u < 0L) {
                u = Long.MAX_VALUE;
            }
            requested = u;
            Flow.Subscription s = currentSubscription;
            if (decrementAndGet() != 0) {
                arbiterDrainLoop();
            }
            if (s != null) {
                s.request(n);
            }
        } else {
            SubscriptionHelper.addRequested(this, MISSED_REQUESTED, n);
            if (getAndIncrement() != 0) {
                return;
            }
            arbiterDrainLoop();
        }
    }

    @Override
    public void cancel() {
        CANCELLED.setRelease(this, true);
        if (getAcquire() == 0 && compareAndSet(0, 1)) {
            Flow.Subscription s = currentSubscription;
            currentSubscription = SubscriptionHelper.CANCELLED;
            if (s != null) {
                s.cancel();
            }
            if (decrementAndGet() != 0) {
                arbiterDrainLoop();
            }
        } else {
            if (getAndIncrement() != 0) {
                return;
            }
            arbiterDrainLoop();
        }
    }

    void arbiterDrainLoop() {
        long toRequest = 0L;
        Flow.Subscription requestFrom = null;

        int missed = 1;
        for (;;) {
            long mr = (long)MISSED_REQUESTED.getAcquire(this);
            if (mr != 0L) {
                mr = (long)MISSED_REQUESTED.getAndSet(this, 0L);
            }

            Flow.Subscription curr = currentSubscription;

            Flow.Subscription ms = missedSubscription;
            if (ms != null) {
                missedSubscription = null;
            }

            long mp = missedProduced;
            if (mp != 0L) {
                missedProduced = 0L;
            }

            if ((boolean)CANCELLED.getAcquire(this)) {
                if (curr != null) {
                    currentSubscription = SubscriptionHelper.CANCELLED;
                    curr.cancel();
                }
                if (ms != null) {
                    ms.cancel();
                }
            } else {
                long r = requested;
                if (r != Long.MAX_VALUE) {
                    long u = r + mr;
                    if (u < 0L) {
                        r = Long.MAX_VALUE;
                        requested = Long.MAX_VALUE;
                    } else {
                        long v = u - mp;
                        if (v <= 0L) {
                            v = 0L;
                        }
                        r = v;
                        requested = v;
                    }
                }

                if (ms != null) {
                    currentSubscription = ms;
                    toRequest = r;
                    requestFrom = ms;
                } else {
                    toRequest += mr;
                    if (toRequest < 0L) {
                        toRequest = Long.MAX_VALUE;
                    }
                    requestFrom = curr;
                }
            }


            missed = addAndGet(-missed);
            if (missed == 0) {
                break;
            }
        }

        if (requestFrom != null && toRequest != 0L) {
            requestFrom.request(toRequest);
        }
    }
}
