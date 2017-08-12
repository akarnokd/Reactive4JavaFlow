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
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;

public final class FolyamBufferSize<T, C extends Collection<? super T>> extends Folyam<C> {

    final Folyam<T> source;

    final int size;

    final int skip;

    final Callable<C> collectionSupplier;

    public FolyamBufferSize(Folyam<T> source, int size, int skip, Callable<C> collectionSupplier) {
        this.source = source;
        this.size = size;
        this.skip = skip;
        this.collectionSupplier = collectionSupplier;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super C> s) {
        if (size == skip) {
            source.subscribe(new BufferSizeExactSubscriber<>(s, collectionSupplier, size));
        } else if (size > skip) {
            source.subscribe(new BufferOverlapSubscriber<>(s, collectionSupplier, size, skip));
        } else {
            source.subscribe(new BufferSkipSubscriber<>(s, collectionSupplier, size, skip));
        }
    }

    static final class BufferSizeExactSubscriber<T, C extends Collection<? super T>> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super C> actual;

        final Callable<C> collectionSupplier;

        final int size;

        int count;

        Flow.Subscription upstream;

        C buffer;

        BufferSizeExactSubscriber(FolyamSubscriber<? super C> actual, Callable<C> collectionSupplier, int size) {
            this.actual = actual;
            this.collectionSupplier = collectionSupplier;
            this.size = size;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            int c = count;
            C buf = buffer;
            if (c == 0) {
                try {
                    buf = Objects.requireNonNull(collectionSupplier.call(), "The collectionSupplier returned a null collection");
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
                buffer = buf;
            }

            buf.add(item);

            if (++c == size) {
                count = 0;
                buffer = null;
                actual.onNext(buf);
            } else {
                count = c;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            buffer = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            C b = buffer;
            buffer = null;
            if (count != 0) {
                actual.onNext(b);
            }
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.request(SubscriptionHelper.multiplyCap(size, n));
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }

    static final class BufferSkipSubscriber<T, C extends Collection<? super T>> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super C> actual;

        final Callable<C> collectionSupplier;

        final int size;

        final int skip;

        int index;

        int count;

        Flow.Subscription upstream;

        C buffer;

        boolean once;
        static final VarHandle ONCE;

        static {
            try {
                ONCE = MethodHandles.lookup().findVarHandle(BufferSkipSubscriber.class, "once", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        BufferSkipSubscriber(FolyamSubscriber<? super C> actual, Callable<C> collectionSupplier, int size, int skip) {
            this.actual = actual;
            this.collectionSupplier = collectionSupplier;
            this.size = size;
            this.skip = skip;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            C buf = buffer;
            int idx = index;
            if (idx == 0) {
                try {
                    buf = Objects.requireNonNull(collectionSupplier.call(), "The collectionSupplier returned a null collection");
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
                buffer = buf;
            }

            if (buf != null) {
                buf.add(item);

                int c = count + 1;
                if (c == size) {
                    count = 0;
                    buffer = null;
                    actual.onNext(buf);
                } else {
                    count = c;
                }
            }

            if (++idx == skip) {
                count = 0;
                index = 0;
            } else {
                index = idx;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            buffer = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            C b = buffer;
            buffer = null;
            if (count != 0) {
                actual.onNext(b);
            }
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            if (!(boolean)ONCE.getAcquire(this) && ONCE.compareAndSet(this, false, true)) {
                long a = SubscriptionHelper.multiplyCap(n - 1, skip);
                long b = SubscriptionHelper.addCap(a, size);
                upstream.request(b);
            } else {
                long a = SubscriptionHelper.multiplyCap(n, skip);
                upstream.request(a);
            }
        }

        @Override
        public void cancel() {
            upstream.cancel();
        }
    }

    static final class BufferOverlapSubscriber<T, C extends Collection<? super T>> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super C> actual;

        final Callable<C> collectionSupplier;

        final int size;

        final int skip;

        ArrayDeque<C> buffers;

        int index;

        int count;

        long produced;

        Flow.Subscription upstream;

        boolean once;
        static final VarHandle ONCE;

        long requested;
        static final VarHandle REQUESTED;

        boolean cancelled;
        static final VarHandle CANCELLED;

        static {
            try {
                ONCE = MethodHandles.lookup().findVarHandle(BufferOverlapSubscriber.class, "once", boolean.class);
                REQUESTED = MethodHandles.lookup().findVarHandle(BufferOverlapSubscriber.class, "requested", long.class);
                CANCELLED = MethodHandles.lookup().findVarHandle(BufferOverlapSubscriber.class, "cancelled", boolean.class);
            } catch (Throwable ex) {
                throw new InternalError(ex);
            }
        }

        BufferOverlapSubscriber(FolyamSubscriber<? super C> actual, Callable<C> collectionSupplier, int size, int skip) {
            this.actual = actual;
            this.collectionSupplier = collectionSupplier;
            this.size = size;
            this.skip = skip;
            this.buffers = new ArrayDeque<>();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            ArrayDeque<C> bufs = buffers;
            if (bufs == null) {
                return;
            }

            int idx = index;
            if (idx == 0) {
                try {
                    bufs.offer(Objects.requireNonNull(collectionSupplier.call(), "The collectionSupplier returned a null collection"));
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
            }

            for (C b : bufs) {
                b.add(item);
            }

            int c = count + 1;
            if (c == size) {
                count = c - skip;
                produced++;
                actual.onNext(bufs.poll());
            } else {
                count = c;
            }

            if (++idx == skip) {
                index = 0;
            } else {
                index = idx;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (buffers == null) {
                FolyamPlugins.onError(throwable);
                return;
            }
            buffers = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            ArrayDeque<C> bufs = buffers;
            if (bufs == null) {
                return;
            }
            long p = produced;
            if (p != 0L) {
                SubscriptionHelper.produced(this, REQUESTED, p);
            }
            SubscriptionHelper.postComplete(actual, bufs, this, REQUESTED, CANCELLED);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.postCompleteRequest(actual, buffers, this, REQUESTED, CANCELLED, n)) {
                if (!(boolean)ONCE.getAcquire(this) && ONCE.compareAndSet(this, false, true)) {
                    long a = SubscriptionHelper.multiplyCap(n - 1, skip);
                    long b = SubscriptionHelper.addCap(a, size);
                    upstream.request(b);
                } else {
                    long a = SubscriptionHelper.multiplyCap(n, skip);
                    upstream.request(a);
                }
            }
        }

        @Override
        public void cancel() {
            CANCELLED.setVolatile(this, true);
            upstream.cancel();
        }
    }
}
