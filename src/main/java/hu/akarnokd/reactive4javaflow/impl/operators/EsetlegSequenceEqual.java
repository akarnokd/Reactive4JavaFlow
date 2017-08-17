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
import hu.akarnokd.reactive4javaflow.functionals.CheckedBiPredicate;
import hu.akarnokd.reactive4javaflow.fused.FusedQueue;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;

public final class EsetlegSequenceEqual<T> extends Esetleg<Boolean> {

    final Flow.Publisher<? extends T> first;

    final Flow.Publisher<? extends T> second;

    final CheckedBiPredicate<? super T, ? super T> isEqual;

    final int prefetch;

    public EsetlegSequenceEqual(Flow.Publisher<? extends T> first, Flow.Publisher<? extends T> second, CheckedBiPredicate<? super T, ? super T> isEqual, int prefetch) {
        this.first = first;
        this.second = second;
        this.isEqual = isEqual;
        this.prefetch = prefetch;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Boolean> s) {
        SequenceEqualCoordinator<T> parent = new SequenceEqualCoordinator<>(s, isEqual, first instanceof Esetleg ? 1 : prefetch, second instanceof Esetleg ? 1 : prefetch);
        s.onSubscribe(parent);
        parent.subscribe(first, second);
    }

    static final class SequenceEqualCoordinator<T> extends DeferredScalarSubscription<Boolean> implements QueuedFolyamSubscriberSupport<T> {

        final CheckedBiPredicate<? super T, ? super T> isEqual;

        final QueuedInnerFolyamSubscriber<T> sub1;

        final QueuedInnerFolyamSubscriber<T> sub2;

        int wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), SequenceEqualCoordinator.class, "wip", int.class);

        Throwable error;
        static final VarHandle ERROR = VH.find(MethodHandles.lookup(), SequenceEqualCoordinator.class, "error", Throwable.class);

        T value1;
        T value2;

        SequenceEqualCoordinator(FolyamSubscriber<? super Boolean> actual, CheckedBiPredicate<? super T, ? super T> isEqual, int prefetch1, int prefetch2) {
            super(actual);
            this.isEqual = isEqual;
            this.sub1 = new QueuedInnerFolyamSubscriber<>(this, 0, prefetch1);
            this.sub2 = new QueuedInnerFolyamSubscriber<>(this, 1, prefetch2);
        }

        void subscribe(Flow.Publisher<? extends T> s1, Flow.Publisher<? extends T> s2) {
            s1.subscribe(sub1);
            s2.subscribe(sub2);
        }

        @Override
        public void innerError(QueuedInnerFolyamSubscriber<T> sender, int index, Throwable ex) {
            if (ExceptionHelper.addThrowable(this, ERROR, ex)) {
                if (index == 0) {
                    sub2.cancel();
                } else {
                    sub1.cancel();
                }
                sender.setDone();
                drain();
            } else {
                FolyamPlugins.onError(ex);
            }
        }

        @Override
        public void drain() {
            if ((int)WIP.getAndAdd(this, 1) != 0) {
                return;
            }

            int missed = 1;
            QueuedInnerFolyamSubscriber<T> sub1 = this.sub1;
            QueuedInnerFolyamSubscriber<T> sub2 = this.sub2;
            CheckedBiPredicate<? super T, ? super T> pred = this.isEqual;

            for (;;) {

                for (;;) {
                    if (isCancelled()) {
                        clear();
                        return;
                    }

                    if (ERROR.getAcquire(this) != null) {
                        Throwable ex = ExceptionHelper.terminate(this, ERROR);
                        clear();
                        error(ex);
                        return;
                    }

                    boolean d1 = sub1.isDone();
                    T v1 = value1;
                    if (v1 == null) {
                        FusedQueue<T> q = sub1.getQueue();
                        try {
                            v1 = q != null ? q.poll() : null;
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            sub1.cancel();
                            sub2.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            clear();
                            error(ex);
                            return;
                        }
                        value1 = v1;
                    }
                    boolean empty1 = v1 == null;

                    boolean d2 = sub2.isDone();
                    T v2 = value2;
                    if (v2 == null) {
                        FusedQueue<T> q = sub2.getQueue();
                        try {
                            v2 = q != null ? q.poll() : null;
                        } catch (Throwable ex) {
                            ExceptionHelper.addThrowable(this, ERROR, ex);
                            sub1.cancel();
                            sub2.cancel();
                            ex = ExceptionHelper.terminate(this, ERROR);
                            clear();
                            error(ex);
                            return;
                        }
                        value2 = v2;
                    }
                    boolean empty2 = v2 == null;

                    if (d1 && d2 && empty1 && empty2) {
                        complete(true);
                        return;
                    }

                    if (d1 && d2 && empty1 != empty2) {
                        sub1.cancel();
                        sub2.cancel();
                        clear();
                        complete(false);
                        return;
                    }

                    if (empty1 || empty2) {
                        break;
                    }

                    boolean b;
                    try {
                        b = pred.test(v1, v2);
                    } catch (Throwable ex) {
                        ExceptionHelper.addThrowable(this, ERROR, ex);
                        sub1.cancel();
                        sub2.cancel();
                        ex = ExceptionHelper.terminate(this, ERROR);
                        clear();
                        error(ex);
                        return;
                    }
                    if (!b) {
                        sub1.cancel();
                        sub2.cancel();
                        clear();
                        complete(false);
                        return;
                    }
                    value1 = null;
                    value2 = null;
                    sub1.request();
                    sub2.request();
                }

                missed = (int)WIP.getAndAdd(this, -missed) - missed;
                if (missed == 0) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            sub1.cancel();
            sub2.cancel();
            if ((int)WIP.getAndAdd(this, 1) == 0) {
                clear();
            }
        }

        public void clear() {
            super.clear();
            value1 = null;
            value2 = null;
            sub1.clear();
            sub2.clear();
        }

    }
}
