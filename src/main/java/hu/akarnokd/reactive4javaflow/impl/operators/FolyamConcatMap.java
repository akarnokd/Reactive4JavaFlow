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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class FolyamConcatMap<T, R> extends Folyam<R> {

    final Folyam<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int prefetch;

    final boolean delayError;

    public FolyamConcatMap(Folyam<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
        this.source = source;
        this.mapper = mapper;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        source.subscribe(new ConcatMapSubscriber<>(s, mapper, prefetch, delayError));
    }

    static final class ConcatMapSubscriber<T, R> extends SubscriptionArbiter implements FolyamSubscriber<T> {

        final FolyamSubscriber<? super R> actual;

        final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        final int prefetch;

        final boolean delayError;

        final AbstractConcatInnerSubscriber<R> inner;

        Flow.Subscription upstream;

        FusedQueue<T> queue;

        int consumed;

        boolean done;
        static final VarHandle DONE;

        volatile boolean cancelled;

        volatile boolean active;
        static final VarHandle ACTIVE;

        int wip;
        static final VarHandle WIP;

        Throwable error;
        static final VarHandle ERROR;

        int fusionMode;

        static {
            try {
                DONE = MethodHandles.lookup().findVarHandle(ConcatMapSubscriber.class, "done", Boolean.TYPE);
                ACTIVE = MethodHandles.lookup().findVarHandle(ConcatMapSubscriber.class, "active", Boolean.TYPE);
                WIP = MethodHandles.lookup().findVarHandle(ConcatMapSubscriber.class, "wip", Integer.TYPE);
                ERROR = MethodHandles.lookup().findVarHandle(ConcatMapSubscriber.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        ConcatMapSubscriber(FolyamSubscriber<? super R> actual, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int prefetch, boolean delayError) {
            this.actual = actual;
            this.mapper = mapper;
            this.prefetch = prefetch;
            this.delayError = delayError;
            if (actual instanceof ConditionalSubscriber) {
                this.inner = new ConcatInnerConditionalSubscriber<>((ConditionalSubscriber<? super R>) actual, this);
            } else {
                this.inner = new ConcatInnerSubscriber<>(actual, this);
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            if (subscription instanceof FusedSubscription) {
                FusedSubscription fs = (FusedSubscription) subscription;

                int m = fs.requestFusion(FusedSubscription.ANY);
                if (m == FusedSubscription.SYNC) {
                    fusionMode = m;
                    queue = fs;
                    DONE.setRelease(this, true);

                    actual.onSubscribe(this);

                    drain();
                    return;
                }
                if (m == FusedSubscription.ASYNC) {
                    fusionMode = m;
                    queue = fs;

                    actual.onSubscribe(this);

                    subscription.request(prefetch);
                    return;
                }
            }

            queue = new SpscArrayQueue<>(prefetch);

            actual.onSubscribe(this);

            subscription.request(prefetch);
        }

        @Override
        public void onNext(T item) {
            if (fusionMode == FusedSubscription.NONE) {
                queue.offer(item);
            }
            drain();
        }

        @Override
        public void onError(Throwable throwable) {
            if (ExceptionHelper.addThrowable(this, ERROR, throwable)) {
                DONE.setRelease(this, true);
                drain();
            } else {
                FolyamPlugins.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            DONE.setRelease(this, true);
            drain();
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            int limit = prefetch - (prefetch >> 2);

            do {
                if (cancelled) {
                    queue.clear();
                    return;
                }
                if (!active) {
                    boolean d = (boolean)DONE.getAcquire(this);
                    Flow.Publisher<? extends R> fp = null;

                    if (d && !delayError && ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        actual.onError(ex);
                        return;
                    }

                    T v;
                    try {
                        v = queue.poll();

                        if (v != null) {
                            fp = Objects.requireNonNull(mapper.apply(v), "The mapper returned a null Flow.Publisher");
                        }
                    } catch (Throwable throwable) {
                        ExceptionHelper.addThrowable(this, ERROR, throwable);
                        upstream.cancel();
                        queue.clear();
                        d = true;
                        v = null;
                    }

                    if (d && v == null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        if (ex == null) {
                            actual.onComplete();
                        } else {
                            actual.onError(ex);
                        }
                        return;
                    }

                    if (v != null) {

                        if (fusionMode != FusedSubscription.SYNC) {
                            int c = consumed + 1;
                            if (c == limit) {
                                consumed = 0;
                                upstream.request(limit);
                            } else {
                                consumed = c;
                            }
                        }

                        long ip = inner.produced;
                        if (ip != 0L) {
                            inner.produced = 0L;
                            arbiterProduced(ip);
                        }

                        ACTIVE.setRelease(this, true);
                        fp.subscribe(inner);
                    }
                }
            } while ((int)WIP.getAndAdd(this, -1) - 1 != 0);
        }

        @Override
        public void cancel() {
            cancelled = true;
            super.cancel();
            upstream.cancel();
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                queue.clear();
            }
        }

        static abstract class AbstractConcatInnerSubscriber<R> implements FolyamSubscriber<R> {

            final ConcatMapSubscriber<?, ?> parent;

            long produced;

            AbstractConcatInnerSubscriber(ConcatMapSubscriber<?, ?> parent) {
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                parent.arbiterReplace(subscription);
            }

            @Override
            public void onError(Throwable throwable) {
                ConcatMapSubscriber<?, ?> parent = this.parent;
                ACTIVE.setRelease(parent, false);
                parent.onError(throwable);
            }

            @Override
            public void onComplete() {
                ConcatMapSubscriber<?, ?> parent = this.parent;
                ACTIVE.setRelease(parent, false);
                parent.drain();
            }
        }

        static final class ConcatInnerSubscriber<R> extends AbstractConcatInnerSubscriber<R> {

            final FolyamSubscriber<? super R> actual;

            ConcatInnerSubscriber(FolyamSubscriber<? super R> actual, ConcatMapSubscriber<?, ?> parent) {
                super(parent);
                this.actual = actual;
            }

            @Override
            public void onNext(R item) {
                produced++;
                actual.onNext(item);
            }
        }

        static final class ConcatInnerConditionalSubscriber<R> extends AbstractConcatInnerSubscriber<R> implements ConditionalSubscriber<R> {

            final ConditionalSubscriber<? super R> actual;

            ConcatInnerConditionalSubscriber(ConditionalSubscriber<? super R> actual, ConcatMapSubscriber<?, ?> parent) {
                super(parent);
                this.actual = actual;
            }

            @Override
            public void onNext(R item) {
                produced++;
                actual.onNext(item);
            }

            @Override
            public boolean tryOnNext(R item) {
                if (actual.tryOnNext(item)) {
                    produced++;
                    return true;
                }
                return false;
            }
        }
    }
}
