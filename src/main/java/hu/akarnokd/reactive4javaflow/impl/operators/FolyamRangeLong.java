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

package hu.akarnokd.reactive4javaflow.impl.operators;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.util.concurrent.atomic.AtomicLong;

public final class FolyamRangeLong extends Folyam<Long> {

    final long start;

    final long end;

    public FolyamRangeLong(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Long> s) {
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new RangeLongConditionalSubscription((ConditionalSubscriber<? super Long>)s, start, end));
        } else {
            s.onSubscribe(new RangeLongSubscription(s, start, end));
        }
    }

    static abstract class AbstractRangeLongSubscription extends AtomicLong implements FusedSubscription<Long> {

        final long end;

        long index;

        volatile boolean cancelled;

        AbstractRangeLongSubscription(long start, long end) {
            this.index = start;
            this.end = end;
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final Long poll() throws Throwable {
            long idx = index;
            if (idx == end) {
                return null;
            }
            index = idx + 1;
            return idx;
        }

        @Override
        public final boolean isEmpty() {
            return index == end;
        }

        @Override
        public final void clear() {
            index = end;
        }

        @Override
        public final void request(long n) {
            if (SubscriptionHelper.addRequested(this, n) == 0) {
                if (n == Long.MAX_VALUE) {
                    fastPath();
                } else {
                    slowPath(n);
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        abstract void fastPath();

        abstract void slowPath(long n);
    }

    static final class RangeLongSubscription extends AbstractRangeLongSubscription {

        final FolyamSubscriber<? super Long> actual;

        RangeLongSubscription(FolyamSubscriber<? super Long> actual, long start, long end) {
            super(start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super Long> a = actual;
            long e = end;
            for (long i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                a.onNext(i);
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super Long> a = actual;
            long idx = index;
            long e = 0L;
            long f = end;
            for (;;) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    a.onNext(idx);

                    idx++;
                    e++;
                }

                if (idx == f) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }

                n = getAcquire();
                if (e == n) {
                    index = idx;
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0;
                }
            }
        }
    }

    static final class RangeLongConditionalSubscription extends AbstractRangeLongSubscription {

        final ConditionalSubscriber<? super Long> actual;

        RangeLongConditionalSubscription(ConditionalSubscriber<? super Long> actual, long start, long end) {
            super(start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super Long> a = actual;
            long e = end;
            for (long i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                a.tryOnNext(i);
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super Long> a = actual;
            long idx = index;
            long e = 0L;
            long f = end;
            for (;;) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    if (a.tryOnNext(idx)) {
                        e++;
                    }

                    idx++;
                }

                if (idx == f) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }

                n = getAcquire();
                if (e == n) {
                    index = idx;
                    n = addAndGet(-e);
                    if (n == 0L) {
                        break;
                    }
                    e = 0;
                }
            }
        }
    }
}
