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
import hu.akarnokd.reactive4javaflow.fused.FusedQueue;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.*;

import static java.lang.invoke.MethodHandles.lookup;

public final class FolyamOrderedMergeArray<T> extends Folyam<T> {
    final Flow.Publisher<? extends T>[] sources;

    final Comparator<? super T> comparator;

    final boolean delayErrors;

    final int prefetch;

    public FolyamOrderedMergeArray(Flow.Publisher<? extends T>[] sources,
                                   Comparator<? super T> comparator,
                                   int prefetch,
                                   boolean delayErrors) {
        this.sources = sources;
        this.comparator = comparator;
        this.prefetch = prefetch;
        this.delayErrors = delayErrors;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Flow.Publisher<? extends T>[] array = sources;
        int n = array.length;
        subscribe(s, array, n, comparator, prefetch, delayErrors);
    }

    public static <T> void subscribe(FolyamSubscriber<? super T> s, Flow.Publisher<? extends T>[] array, int n, Comparator<? super T> comparator, int prefetch, boolean delayErrors) {
        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }

        if (n == 1) {
            array[0].subscribe(s);
            return;
        }

        MergeCoordinator<T> parent = new MergeCoordinator<>(s, comparator, n, prefetch, delayErrors);
        s.onSubscribe(parent);
        parent.subscribe(array, n);

    }

    static final class MergeCoordinator<T>
            extends AtomicInteger
            implements Flow.Subscription, QueuedFolyamSubscriberSupport<T> {
        private static final long serialVersionUID = -8467324377226330554L;

        final FolyamSubscriber<? super T> actual;

        final Comparator<? super T> comparator;

        final QueuedInnerFolyamSubscriber<T>[] subscribers;

        final boolean delayErrors;

        final Object[] latest;

        volatile boolean cancelled;

        long requested;
        static final VarHandle REQUESTED;

        Throwable error;
        static final VarHandle ERROR;

        long emitted;

        static {
            Lookup lk = lookup();
            try {
                REQUESTED = lk.findVarHandle(MergeCoordinator.class, "requested", long.class);
                ERROR = lk.findVarHandle(MergeCoordinator.class, "error", Throwable.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }


        @SuppressWarnings("unchecked")
        MergeCoordinator(FolyamSubscriber<? super T> actual, Comparator<? super T> comparator, int n, int prefetch, boolean delayErrors) {
            this.actual = actual;
            this.comparator = comparator;
            this.delayErrors = delayErrors;
            QueuedInnerFolyamSubscriber<T>[] subs = new QueuedInnerFolyamSubscriber[n];
            for (int i = 0; i < n; i++) {
                subs[i] = new QueuedInnerFolyamSubscriber<>(this, i, prefetch);
            }
            this.subscribers = subs;
            this.latest = new Object[n];
        }

        void subscribe(Flow.Publisher<? extends T>[] sources, int n) {
            QueuedInnerFolyamSubscriber<T>[] subs = subscribers;
            for (int i = 0; i < n && !cancelled; i++) {
                Flow.Publisher<? extends T> p = sources[i];
                if (p != null) {
                    p.subscribe(subs[i]);
                } else {
                    EmptySubscription.error(subs[i], new NullPointerException("The " + i + "th source is null"));
                    if (!delayErrors) {
                        break;
                    }
                }
            }
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.addRequested(this, REQUESTED, n);
            drain();
        }

        void cancelSources() {
            for (QueuedInnerFolyamSubscriber<T> d : subscribers) {
                d.cancel();
            }
        }

        void clearSources() {
            Arrays.fill(latest, this);
            for (QueuedInnerFolyamSubscriber<T> d : subscribers) {
                FusedQueue<T> q = d.getQueue();
                if (q != null) {
                    q.clear();
                }
            }
        }

        void cancelAndClearSources() {
            Arrays.fill(latest, this);
            for (QueuedInnerFolyamSubscriber<T> d : subscribers) {
                d.cancel();
                FusedQueue<T> q = d.getQueue();
                if (q != null) {
                    q.clear();
                }
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelSources();
                if (getAndIncrement() == 0) {
                    clearSources();
                }
            }
        }

        @Override
        public void innerError(QueuedInnerFolyamSubscriber<T> inner, int index, Throwable e) {
            if (ExceptionHelper.addThrowable(this, ERROR, e)) {
                if (!delayErrors) {
                    cancelSources();
                } else {
                    inner.setDone();
                }
                drain();
            } else {
                FolyamPlugins.onError(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void drain() {
            if (getAndIncrement() != 0) {
                return;
            }

            int missed = 1;

            FolyamSubscriber<? super T> a = actual;
            QueuedInnerFolyamSubscriber<T>[] subs = subscribers;
            int n = subs.length;
            Object[] latest = this.latest;
            Comparator<? super T> comp = comparator;
            long e = emitted;

            for (;;) {

                long r = (long)REQUESTED.getAcquire(this);

                while (e != r) {
                    if (cancelled) {
                        clearSources();
                        return;
                    }

                    if (!delayErrors && ERROR.getAcquire(this) != null) {
                        cancelAndClearSources();
                        a.onError(ExceptionHelper.terminate(this, ERROR));
                        return;
                    }

                    boolean d = true;
                    int hasValue = 0;
                    boolean empty = true;

                    T smallest = null;
                    int pick = -1;

                    for (int i = 0; i < n; i++) {
                        QueuedInnerFolyamSubscriber<T> inner = subs[i];
                        boolean innerDone = inner.isDone();
                        if (!innerDone) {
                            d = false;
                        }
                        Object v = latest[i];
                        if (v == null) {
                            FusedQueue<T> q = inner.getQueue();
                            try {
                                v = q != null ? q.poll() : null;
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayErrors) {
                                    cancelAndClearSources();
                                    a.onError(ExceptionHelper.terminate(this, ERROR));
                                    return;
                                }
                                inner.setDone();
                                v = this;
                            }

                            if (v != null) {
                                latest[i] = v;
                                hasValue++;
                                empty = false;
                            } else
                            if (innerDone) {
                                latest[i] = this;
                                hasValue++;
                            }
                        } else {
                            hasValue++;
                            if (v != this) {
                                empty = false;
                            }
                        }

                        if (v != null && v != this) {
                            boolean smaller;
                            try {
                                smaller = smallest == null || comp.compare(smallest, (T) v) > 0;
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                cancelAndClearSources();
                                a.onError(ExceptionHelper.terminate(this, ERROR));
                                return;
                            }
                            if (smaller) {
                                smallest = (T)v;
                                pick = i;
                            }
                        }
                    }

                    if (hasValue == n && pick >= 0) {
                        a.onNext(smallest);
                        latest[pick] = null;
                        subs[pick].request();

                        e++;
                    } else {
                        if (d && empty) {
                            if (ERROR.getAcquire(this) != null) {
                                a.onError(ExceptionHelper.terminate(this, ERROR));
                            } else {
                                a.onComplete();
                            }
                            return;
                        }
                        break;
                    }
                }

                if (e == r) {
                    if (cancelled) {
                        clearSources();
                        return;
                    }

                    if (!delayErrors && ERROR.getAcquire(this) != null) {
                        cancelAndClearSources();
                        a.onError(ExceptionHelper.terminate(this, ERROR));
                        return;
                    }

                    boolean d = true;
                    boolean empty = true;

                    for (int i = 0; i < subs.length; i++) {
                        QueuedInnerFolyamSubscriber<T> inner = subs[i];
                        if (!inner.isDone()) {
                            d = false;
                            break;
                        }
                        Object o = latest[i];
                        FusedQueue<T> q = inner.getQueue();
                        if (o == null && q != null) {
                            try {
                                o = q.poll();
                            } catch (Throwable ex) {
                                FolyamPlugins.handleFatal(ex);
                                ExceptionHelper.addThrowable(this, ERROR, ex);
                                if (!delayErrors) {
                                    cancelAndClearSources();
                                    a.onError(ex);
                                    return;
                                }
                                o = this;
                            }

                            latest[i] = o;
                        }
                        if (o != null && o != this) {
                            empty = false;
                            break;
                        }
                    }

                    if (d && empty) {
                        if (ERROR.getAcquire(this) != null) {
                            a.onError(ExceptionHelper.terminate(this, ERROR));
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                }

                emitted = e;
                missed = addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }
    }}
