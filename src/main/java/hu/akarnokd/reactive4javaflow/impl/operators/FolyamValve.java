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
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscLinkedArrayQueue;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamValve<T> extends Folyam<T> {

    final Folyam<T> source;

    final Flow.Publisher<Boolean> other;

    final boolean defaultOpen;

    final int bufferSize;

    public FolyamValve(Folyam<T> source, Flow.Publisher<Boolean> other, int bufferSize, boolean defaultOpen) {
        this.source = source;
        this.other = other;
        this.bufferSize = bufferSize;
        this.defaultOpen = defaultOpen;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        ValveMainSubscriber<T> parent = new ValveMainSubscriber<T>(s, bufferSize, defaultOpen);
        s.onSubscribe(parent);
        other.subscribe(parent.other);
        source.subscribe(parent);
    }

    static final class ValveMainSubscriber<T>
            extends AtomicInteger
            implements FolyamSubscriber<T>, Flow.Subscription {

        private static final long serialVersionUID = -2233734924340471378L;

        final FolyamSubscriber<? super T> actual;

        final PlainQueue<T> queue;

        final OtherSubscriber other;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM;

        long requested;
        static final VarHandle REQUESTED;

        Throwable error;
        static final VarHandle ERROR;

        boolean done;
        static final VarHandle DONE;

        volatile boolean gate;

        volatile boolean cancelled;

        static {
            Lookup lk = lookup();
            try {
                UPSTREAM = lk.findVarHandle(ValveMainSubscriber.class, "upstream", Flow.Subscription.class);
                REQUESTED = lk.findVarHandle(ValveMainSubscriber.class, "requested", long.class);
                ERROR = lk.findVarHandle(ValveMainSubscriber.class, "error", Throwable.class);
                DONE = lk.findVarHandle(ValveMainSubscriber.class, "done", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }


        ValveMainSubscriber(FolyamSubscriber<? super T> actual, int bufferSize, boolean defaultOpen) {
            this.actual = actual;
            this.queue = new SpscLinkedArrayQueue<>(bufferSize);
            this.gate = defaultOpen;
            this.other = new OtherSubscriber();
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            SubscriptionHelper.deferredReplace(this, UPSTREAM, REQUESTED, s);
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            if (ExceptionHelper.addThrowable(this, ERROR, t)) {
                SubscriptionHelper.cancel(other);
                drain();
            } else {
                FolyamPlugins.onError(t);
            }
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this, UPSTREAM, REQUESTED, n);
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this, UPSTREAM);
            SubscriptionHelper.cancel(other);
        }

        void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;

            PlainQueue<T> q = queue;
            FolyamSubscriber<? super T> a = actual;

            for (;;) {
                for (;;) {
                    if (cancelled) {
                        q.clear();
                        return;
                    }

                    if (ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        q.clear();
                        a.onError(ex);
                        return;
                    }

                    if (!gate) {
                        break;
                    }

                    boolean d = done;
                    T v = q.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        SubscriptionHelper.cancel(other);
                        a.onComplete();
                        return;
                    }

                    if (empty) {
                        break;
                    }

                    a.onNext(v);
                }

                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        void change(boolean state) {
            gate = state;
            if (state) {
                drain();
            }
        }

        void innerError(Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                SubscriptionHelper.cancel(this, UPSTREAM);
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        void innerComplete() {
            innerError(new IllegalStateException("The valve source completed unexpectedly."));
        }

        final class OtherSubscriber extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<Boolean> {

            private static final long serialVersionUID = -3076915855750118155L;

            @Override
            public void onSubscribe(Flow.Subscription s) {
                if (SubscriptionHelper.replace(this, s)) {
                    s.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(Boolean t) {
                change(t);
            }

            @Override
            public void onError(Throwable t) {
                innerError(t);
            }

            @Override
            public void onComplete() {
                innerComplete();
            }
        }
    }
}
