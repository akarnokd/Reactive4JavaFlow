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

public final class FolyamCharacters extends Folyam<Integer> {

    final CharSequence chars;

    final int start;

    final int end;

    public FolyamCharacters(CharSequence chars, int start, int end) {
        this.chars = chars;
        this.start = start;
        this.end = end;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new ArrayConditionalSubscription((ConditionalSubscriber<? super Integer>)s, chars, start, end));
        } else {
            s.onSubscribe(new ArraySubscription(s, chars, start, end));
        }
    }

    static abstract class AbstractArraySubscription extends AtomicLong implements FusedSubscription<Integer> {

        final CharSequence chars;

        final int end;

        int index;

        volatile boolean cancelled;

        AbstractArraySubscription(CharSequence chars, int start, int end) {
            this.chars = chars;
            this.index = start;
            this.end = end;
        }

        @Override
        public final int requestFusion(int mode) {
            return mode & SYNC;
        }

        @Override
        public final Integer poll() throws Throwable {
            int idx = index;
            if (idx == end) {
                return null;
            }
            index = idx + 1;
            return (int)chars.charAt(idx);
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

    static final class ArraySubscription extends AbstractArraySubscription {

        final FolyamSubscriber<? super Integer> actual;

        ArraySubscription(FolyamSubscriber<? super Integer> actual, CharSequence chars, int start, int end) {
            super(chars, start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            FolyamSubscriber<? super Integer> a = actual;
            CharSequence items = chars;
            int e = end;
            for (int i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                a.onNext((int)items.charAt(i));
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            FolyamSubscriber<? super Integer> a = actual;
            CharSequence items = chars;
            int idx = index;
            long e = 0L;
            int f = end;
            for (;;) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    a.onNext((int)items.charAt(idx));

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

    static final class ArrayConditionalSubscription extends AbstractArraySubscription {

        final ConditionalSubscriber<? super Integer> actual;

        ArrayConditionalSubscription(ConditionalSubscriber<? super Integer> actual, CharSequence chars, int start, int end) {
            super(chars, start, end);
            this.actual = actual;
        }

        @Override
        void fastPath() {
            ConditionalSubscriber<? super Integer> a = actual;
            CharSequence items = chars;
            int e = end;
            for (int i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                a.tryOnNext((int)items.charAt(i));
            }
            if (!cancelled) {
                a.onComplete();
            }
        }

        @Override
        void slowPath(long n) {
            ConditionalSubscriber<? super Integer> a = actual;
            CharSequence items = chars;
            int idx = index;
            long e = 0L;
            int f = end;
            for (;;) {

                while (idx != f && e != n) {
                    if (cancelled) {
                        return;
                    }

                    if (a.tryOnNext((int)items.charAt(idx))) {
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
