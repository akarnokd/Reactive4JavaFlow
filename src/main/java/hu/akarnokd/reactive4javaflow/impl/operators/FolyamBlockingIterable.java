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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.util.SpscArrayQueue;

import java.lang.invoke.*;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.locks.*;
import java.util.stream.*;

public final class FolyamBlockingIterable<T> implements Iterable<T> {

    final FolyamPublisher<T> source;

    final int prefetch;

    public FolyamBlockingIterable(FolyamPublisher<T> source, int prefetch) {
        this.source = source;
        this.prefetch = prefetch;
    }

    @Override
    public Iterator<T> iterator() {
        BlockingIterator<T> parent = new BlockingIterator<>(prefetch);
        source.subscribe(parent);
        return parent;
    }

    public static <T> Stream<T> toStream(FolyamPublisher<T> source, int prefetch, boolean parallel) {
        BlockingIterator<T> parent = new BlockingIterator<>(prefetch);
        source.subscribe(parent);

        Spliterator<T> sp = Spliterators.spliterator(parent, 0, 0);
        return StreamSupport.stream(sp, parallel).onClose(parent);
    }

    static final class BlockingIterator<T> extends ReentrantLock implements FolyamSubscriber<T>, Iterator<T>, AutoDisposable, Runnable {

        final int prefetch;

        final int limit;

        final Condition condition;

        FusedQueue<T> queue;
        static final VarHandle QUEUE = VH.find(MethodHandles.lookup(), BlockingIterator.class, "queue", FusedQueue.class);

        boolean done;
        static final VarHandle DONE = VH.find(MethodHandles.lookup(), BlockingIterator.class, "done", Boolean.TYPE);

        Throwable error;

        boolean iteratorDone;
        T iteratorItem;
        Throwable iteratorError;

        int consumed;
        long missed;

        Flow.Subscription upstream;
        static final VarHandle UPSTREAM = VH.find(MethodHandles.lookup(), BlockingIterator.class, "upstream", Flow.Subscription.class);

        long wip;
        static final VarHandle WIP = VH.find(MethodHandles.lookup(), BlockingIterator.class, "wip", Long.TYPE);

        int sourceFused;

        Cleaner.Cleanable cleanable;
        static final VarHandle CLEANABLE = VH.find(MethodHandles.lookup(), BlockingIterator.class, "cleanable", Cleaner.Cleanable.class);

        BlockingIterator(int prefetch) {
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.condition = newCondition();
            this.missed = 1;
        }

        @Override
        public void close() {
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        @Override
        public void run() {
            SubscriptionHelper.cancel(this, UPSTREAM);
        }

        static final class IteratorCleanup implements Runnable {
            final Flow.Subscription upstream;

            IteratorCleanup(Flow.Subscription upstream) {
                this.upstream = upstream;
            }

            @Override
            public void run() {
                upstream.cancel();
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (SubscriptionHelper.replace(this, UPSTREAM, subscription)) {
                CLEANABLE.setRelease(this, CleanerHelper.register(this, new IteratorCleanup(subscription)));

                if (subscription instanceof FusedSubscription) {
                    FusedSubscription<T> fs = (FusedSubscription<T>) subscription;
                    int m = fs.requestFusion(FusedSubscription.ANY | FusedSubscription.BOUNDARY);
                    if (m == FusedSubscription.SYNC) {
                        sourceFused = m;
                        QUEUE.setRelease(this, fs);
                        DONE.setRelease(this, true);
                        signal();
                        return;
                    }
                    if (m == FusedSubscription.ASYNC) {
                        sourceFused = m;
                        QUEUE.setRelease(this, fs);
                        subscription.request(prefetch);
                        return;
                    }
                }

                int p = prefetch;
                QUEUE.setRelease(this, new SpscArrayQueue<>(p));
                subscription.request(p);
            }
        }

        void signal() {
            if ((long)WIP.getAndAdd(this, 1) == 0) {
                lock();
                try {
                    condition.signal();
                } finally {
                    unlock();
                }
            }
        }

        @Override
        public void onNext(T item) {
            if (item != null) {
                queue.offer(item);
            }
            signal();
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
            DONE.setRelease(this, true);
            signal();
        }

        @Override
        public void onComplete() {
            DONE.setRelease(this, true);
            signal();
        }

        void tryClean() {
            Cleaner.Cleanable c = cleanable;
            if (c != null) {
                c.clean();
            }
        }

        @Override
        public boolean hasNext() {
            if (!iteratorDone && iteratorItem == null && iteratorError == null) {
                for (;;) {
                    boolean d = (boolean) DONE.getAcquire(this);
                    T v;
                    try {
                        v = queue.poll();
                    } catch (Throwable ex) {
                        iteratorDone = true;
                        iteratorError = ex;
                        tryClean();
                        queue.clear();
                        return true;
                    }
                    if (v == null) {
                        if (d) {
                            Throwable ex = error;
                            if (ex != null) {
                                iteratorError = ex;
                                iteratorDone = true;
                                tryClean();
                                return true;
                            }
                            iteratorDone = true;
                            tryClean();
                            return false;
                        }
                        long m = missed;
                        m = (long)WIP.getAndAdd(this, -m) - m;
                        missed = m;
                        if (m == 0L) {
                            lock();
                            try {
                                while ((long) WIP.getAcquire(this) == 0L && !(boolean) DONE.getAcquire(this)) {
                                    condition.await();
                                }
                            } catch (InterruptedException ex) {
                                iteratorError = ex;
                                iteratorDone = true;
                                close();
                                tryClean();
                                return true;
                            } finally {
                                unlock();
                            }
                        }
                    } else {
                        iteratorItem = v;
                        if (sourceFused != FusedSubscription.SYNC) {
                            int c = consumed;
                            int lim = limit;
                            if (++c == lim) {
                                consumed = 0;
                                upstream.request(lim);
                            } else {
                                consumed = c;
                            }
                        }
                        return true;
                    }
                }
            }
            return iteratorItem != null || iteratorError != null;
        }

        @Override
        public T next() {
            if (hasNext()) {
                Throwable e = iteratorError;
                if (e != null) {
                    iteratorError = null;
                    if (e instanceof Error) {
                        throw (Error)e;
                    }
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException)e;
                    }
                    throw new RuntimeException(e);
                }
                T v = iteratorItem;
                if (v != null) {
                    iteratorItem = null;
                    return v;
                }
            }
            throw new NoSuchElementException();
        }
    }
}
