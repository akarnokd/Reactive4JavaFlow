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
import java.util.Arrays;
import java.util.concurrent.Flow;

public final class FolyamConcatArray<T> extends Folyam<T> {

    final Flow.Publisher<? extends T>[] sources;

    final boolean delayError;

    public FolyamConcatArray(Flow.Publisher<? extends T>[] sources, boolean delayError) {
        this.sources = sources;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        AbstractConcatArray<T> parent;
        if (s instanceof ConditionalSubscriber) {
            parent = new ConcatArrayConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, sources, delayError);
        } else {
            parent = new ConcatArraySubscriber<>(s, sources, delayError);
        }
        s.onSubscribe(parent);
        parent.drain();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Folyam<T> concatWith(Folyam<T> source, Flow.Publisher<? extends T> other, boolean delayError) {
        if (source instanceof FolyamConcatArray) {
            FolyamConcatArray f = (FolyamConcatArray) source;
            if (f.delayError == delayError) {
                int n = f.sources.length;
                Flow.Publisher<? extends T>[] b = Arrays.copyOf(f.sources, n + 1);
                b[n] = other;
                return new FolyamConcatArray<>(b, delayError);
            }
        }
        return new FolyamConcatArray<>(new Flow.Publisher[] { source, other }, delayError);
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Folyam<T> startWith(Folyam<T> source, Flow.Publisher<? extends T> other, boolean delayError) {
        if (source instanceof FolyamConcatArray) {
            FolyamConcatArray f = (FolyamConcatArray) source;
            if (f.delayError == delayError) {
                int n = f.sources.length;
                Flow.Publisher<? extends T>[] b = new Flow.Publisher[n + 1];
                System.arraycopy(f.sources, 0, b, 1, n);
                b[0] = other;
                return new FolyamConcatArray<>(b, delayError);
            }
        }
        return new FolyamConcatArray<>(new Flow.Publisher[] { source, other }, delayError);
    }

    static abstract class AbstractConcatArray<T> extends SubscriptionArbiter implements ConditionalSubscriber<T> {

        final Flow.Publisher<? extends T>[] sources;

        final boolean delayError;

        int index;

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), AbstractConcatArray.class, "error", Throwable.class);

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), AbstractConcatArray.class, "wip", Integer.TYPE);

        long produced;

        protected AbstractConcatArray(Flow.Publisher<? extends T>[] sources, boolean delayError) {
            this.sources = sources;
            this.delayError = delayError;
        }

        public final void onSubscribe(Flow.Subscription s) {
            arbiterReplace(s);
        }

        final void drain() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                Flow.Publisher<? extends T>[] srcs = sources;
                int n = srcs.length;
                do {
                    if (arbiterIsCancelled()) {
                        return;
                    }

                    int idx = index;

                    if (idx == srcs.length) {
                        Throwable ex = error;
                        if (ex != null) {
                            error(ex);
                        } else {
                            complete();
                        }
                        return;
                    }

                    Flow.Publisher<? extends T> p = srcs[idx];
                    if (p == null) {
                        error(new NullPointerException("sources[" + idx + "] == null"));
                        return;
                    }

                    index = idx + 1;
                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        arbiterProduced(c);
                    }

                    p.subscribe(this);
                } while ((int)WIP.getAndAdd(this, - 1) - 1 != 0);
            }
        }

        abstract void error(Throwable ex);

        abstract void complete();

        @Override
        public final void onError(Throwable throwable) {
            if (delayError) {
                Throwable ex = error;
                if (ex == null) {
                    error = throwable;
                } else
                if (ex instanceof CompositeThrowable) {
                    error = ((CompositeThrowable)ex).copyAndAdd(throwable);
                } else {
                    error = new CompositeThrowable(ex, throwable);
                }
                drain();
            } else {
                error(throwable);
            }
        }

        @Override
        public final void onComplete() {
            drain();
        }
    }

    static final class ConcatArraySubscriber<T> extends AbstractConcatArray<T> {

        final FolyamSubscriber<? super T> actual;

        protected ConcatArraySubscriber(FolyamSubscriber<? super T> actual, Flow.Publisher<? extends T>[] sources, boolean delayError) {
            super(sources, delayError);
            this.actual = actual;
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        void complete() {
            actual.onComplete();
        }

        @Override
        public boolean tryOnNext(T item) {
            produced++;
            actual.onNext(item);
            return true;
        }

        @Override
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }
    }

    static final class ConcatArrayConditionalSubscriber<T> extends AbstractConcatArray<T> {

        final ConditionalSubscriber<? super T> actual;

        protected ConcatArrayConditionalSubscriber(ConditionalSubscriber<? super T> actual, Flow.Publisher<? extends T>[] sources, boolean delayError) {
            super(sources, delayError);
            this.actual = actual;
        }

        @Override
        void error(Throwable ex) {
            actual.onError(ex);
        }

        @Override
        void complete() {
            actual.onComplete();
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
        public void onNext(T item) {
            produced++;
            actual.onNext(item);
        }
    }
}
