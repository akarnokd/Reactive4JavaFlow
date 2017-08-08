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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.Flow;

public final class FolyamConcatIterable<T> extends Folyam<T> {

    final Iterable<? extends Flow.Publisher<? extends T>> sources;

    final boolean delayError;

    public FolyamConcatIterable(Iterable<? extends Flow.Publisher<? extends T>> sources, boolean delayError) {
        this.sources = sources;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {

        Iterator<? extends Flow.Publisher<? extends T>> it;

        try {
            it = Objects.requireNonNull(sources.iterator(), "The iterable returned a null iterator");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        AbstractConcatIterable<T> parent;

        if (s instanceof ConditionalSubscriber) {
            parent = new ConcatIterableConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, delayError, it);
        } else {
            parent = new ConcatIterableSubscriber<>(s, delayError, it);
        }
        s.onSubscribe(parent);
        parent.drain();
    }

    static abstract class AbstractConcatIterable<T> extends SubscriptionArbiter {

        final boolean delayError;

        final Iterator<? extends Flow.Publisher<? extends T>> sources;

        long produced;

        int wip;
        static final VarHandle WIP;

        Throwable error;

        static {
            try {
                WIP = MethodHandles.lookup().findVarHandle(AbstractConcatIterable.class, "wip", Integer.TYPE);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        AbstractConcatIterable(boolean delayError, Iterator<? extends Flow.Publisher<? extends T>> sources) {
            this.delayError = delayError;
            this.sources = sources;
        }

        public void onSubscribe(Flow.Subscription subscription) {
            arbiterReplace(subscription);
        }

        public void onComplete() {
            drain();
        }

        abstract void drain();
    }

    static final class ConcatIterableSubscriber<T> extends AbstractConcatIterable<T> implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super T> actual;

        ConcatIterableSubscriber(FolyamSubscriber<? super T> actual, boolean delayError, Iterator<? extends Flow.Publisher<? extends T>> sources) {
            super(delayError, sources);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            if (delayError) {
                error = CompositeThrowable.combine(error, throwable);
                drain();
                return;
            }
            actual.onError(throwable);
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    if (arbiterIsCancelled()) {
                        return;
                    }

                    boolean b;

                    try {
                        b = sources.hasNext();
                    } catch (Throwable ex) {
                        ex = CompositeThrowable.combine(error, ex);
                        actual.onError(ex);
                        return;
                    }

                    if (!b) {
                        Throwable ex = error;
                        if (ex == null) {
                            actual.onComplete();
                        } else {
                            actual.onError(ex);
                        }
                        return;
                    }

                    Flow.Publisher<? extends T> p;

                    try {
                        p = Objects.requireNonNull(sources.next(), "The iterator returned a null Flow.Publisher");
                    } catch (Throwable ex) {
                        ex = CompositeThrowable.combine(error, ex);
                        actual.onError(ex);
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        arbiterProduced(c);
                    }

                    p.subscribe(this);
                } while  ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }
    }

    static final class ConcatIterableConditionalSubscriber<T> extends AbstractConcatIterable<T> implements ConditionalSubscriber<T> {

        final ConditionalSubscriber<? super T> actual;

        ConcatIterableConditionalSubscriber(ConditionalSubscriber<? super T> actual, boolean delayError, Iterator<? extends Flow.Publisher<? extends T>> sources) {
            super(delayError, sources);
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }

        @Override
        public boolean tryOnNext(T item) {
            if (actual.tryOnNext(item)) {
                produced++;
                return true;
            }
            return false;
        }

        @Override
        public void onError(Throwable throwable) {
            if (delayError) {
                error = CompositeThrowable.combine(error, throwable);
                drain();
                return;
            }
            actual.onError(throwable);
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                do {
                    if (arbiterIsCancelled()) {
                        return;
                    }

                    boolean b;

                    try {
                        b = sources.hasNext();
                    } catch (Throwable ex) {
                        ex = CompositeThrowable.combine(error, ex);
                        actual.onError(ex);
                        return;
                    }

                    if (!b) {
                        Throwable ex = error;
                        if (ex == null) {
                            actual.onComplete();
                        } else {
                            actual.onError(ex);
                        }
                        return;
                    }

                    Flow.Publisher<? extends T> p;

                    try {
                        p = Objects.requireNonNull(sources.next(), "The iterator returned a null Flow.Publisher");
                    } catch (Throwable ex) {
                        ex = CompositeThrowable.combine(error, ex);
                        actual.onError(ex);
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        arbiterProduced(c);
                    }

                    p.subscribe(this);
                } while  ((int)WIP.getAndAdd(this, -1) - 1 != 0);
            }
        }
    }
}
