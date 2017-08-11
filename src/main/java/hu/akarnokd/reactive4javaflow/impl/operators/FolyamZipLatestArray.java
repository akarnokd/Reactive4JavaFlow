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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamZipLatestArray<T, R> extends Folyam<R> {
    final Flow.Publisher<? extends T>[] sources;

    final CheckedFunction<? super Object[], ? extends R> zipper;

    public FolyamZipLatestArray(Flow.Publisher<? extends T>[] sources,
                                CheckedFunction<? super Object[], ? extends R> zipper) {
        this.sources = sources;
        this.zipper = zipper;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        Flow.Publisher<? extends T>[] srcs = sources;
        int n = srcs.length;
        subscribe(s, srcs, n, zipper);
    }

    public static <T, R> void subscribe(FolyamSubscriber<? super R> s, Flow.Publisher<? extends T>[] srcs, int n, CheckedFunction<? super Object[], ? extends R> zipper) {
        if (n == 0) {
            EmptySubscription.complete(s);
        } else {
            ZipLatestCoordinator<T, R> zc = new ZipLatestCoordinator<T, R>(s, n, zipper);
            s.onSubscribe(zc);

            zc.subscribe(srcs, n);
        }
    }

    static final class ZipLatestCoordinator<T, R> extends AtomicReferenceArray<T> implements Flow.Subscription, Runnable {

        private static final long serialVersionUID = -8321911708267957704L;

        final FolyamSubscriber<? super R> actual;

        final InnerSubscriber<T>[] subscribers;

        final CheckedFunction<? super Object[], ? extends R> zipper;

        int wip;
        static final VarHandle WIP;

        long requested;
        static final VarHandle REQUESTED;

        Throwable error;
        static final VarHandle ERROR;

        volatile boolean cancelled;

        long emitted;

        static {
            Lookup lk = lookup();
            try {
                WIP = lk.findVarHandle(ZipLatestCoordinator.class, "wip", int.class);
                REQUESTED = lk.findVarHandle(ZipLatestCoordinator.class, "requested", long.class);
                ERROR = lk.findVarHandle(ZipLatestCoordinator.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }


        @SuppressWarnings("unchecked")
        ZipLatestCoordinator(FolyamSubscriber<? super R> actual, int n, CheckedFunction<? super Object[], ? extends R> zipper) {
            super(n);
            this.actual = actual;
            this.subscribers = new InnerSubscriber[n];
            for (int i = 0; i < n; i++) {
                subscribers[i] = new InnerSubscriber<T>(this, i);
            }
            this.zipper = zipper;
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            cancelAll();
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                clear();
            }
        }

        void cancelAll() {
            for (InnerSubscriber<?> inner : subscribers) {
                inner.cancel();
            }
        }

        void clear() {
            int n = length();
            for (int i = 0; i < n; i++) {
                lazySet(i, null);
            }
        }

        void drain() {
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                run();
            }
        }

        @Override
        public void run() {
            int missed = 1;
            long e = emitted;
            InnerSubscriber<T>[] subs = subscribers;
            int n = subs.length;
            FolyamSubscriber<? super R> a = actual;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clear();
                        return;
                    }
                    boolean someEmpty = false;
                    for (int i = 0; i < n; i++) {
                        boolean d = subs[i].isDone();
                        Object o = get(i);
                        if (d && o == null) {
                            cancelled = true;
                            cancelAll();
                            clear();
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            if (ex == null) {
                                a.onComplete();
                            } else {
                                a.onError(ex);
                            }
                            return;
                        }
                        if (o == null) {
                            someEmpty = true;
                        }
                    }

                    if (someEmpty) {
                        break;
                    }
                    Object[] array = new Object[n];
                    for (int i = 0; i < n; i++) {
                        array[i] = getAndSet(i, null);
                    }

                    R v;

                    try {
                        v = Objects.requireNonNull(zipper.apply(array), "The zipper returned a null value");
                    } catch (Throwable ex) {
                        FolyamPlugins.handleFatal(ex);
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        cancelled = true;
                        cancelAll();
                        clear();
                        a.onError(ExceptionHelper.terminate(this, ERROR));
                        return;
                    }

                    a.onNext(v);

                    e++;
                }

                if (e == r) {
                    if (cancelled) {
                        clear();
                        return;
                    }

                    for (int i = 0; i < n; i++) {
                        if (subs[i].isDone() && get(i) == null) {
                            cancelled = true;
                            cancelAll();
                            clear();
                            Throwable ex = ExceptionHelper.terminate(this, ERROR);
                            if (ex == null) {
                                a.onComplete();
                            } else {
                                a.onError(ex);
                            }
                            return;
                        }
                    }
                }

                emitted = e;
                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }
        }

        void subscribe(Flow.Publisher<? extends T>[] sources, int n) {
            for (int i = 0; i < n; i++) {
                if (cancelled) {
                    return;
                }
                sources[i].subscribe(subscribers[i]);
            }
        }

        void innerError(InnerSubscriber<T> inner, Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                inner.setDone();
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        static final class InnerSubscriber<T> extends AtomicReference<Flow.Subscription> implements FolyamSubscriber<T> {

            private static final long serialVersionUID = -5384962852497888461L;

            final ZipLatestCoordinator<T, ?> parent;

            final int index;

            boolean done;
            static final VarHandle DONE;

            static {
                Lookup lk = lookup();
                try {
                    DONE = lk.findVarHandle(InnerSubscriber.class, "done", boolean.class);
                } catch (Throwable ex) {
                    throw new InternalError(ex);
                }
            }


            InnerSubscriber(ZipLatestCoordinator<T, ?> parent, int index) {
                this.index = index;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Flow.Subscription s) {
                if (SubscriptionHelper.replace(this, s)) {
                    s.request(Long.MAX_VALUE);
                }
            }

            @Override
            public void onNext(T t) {
                ZipLatestCoordinator<T, ?> p = parent;
                p.lazySet(index, t);
                p.drain();
            }

            @Override
            public void onError(Throwable t) {
                parent.innerError(this, t);
            }

            @Override
            public void onComplete() {
                lazySet(SubscriptionHelper.CANCELLED);
                setDone();
                parent.drain();
            }

            void cancel() {
                SubscriptionHelper.cancel(this);
            }

            boolean isDone() {
                return (boolean)DONE.getAcquire(this);
            }

            void setDone() {
                DONE.setRelease(this, true);
            }
        }
    }
}
