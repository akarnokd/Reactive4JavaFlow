/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.tck;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Flow;

import hu.akarnokd.reactive4javaflow.Folyam;
import org.reactivestreams.*;
import org.reactivestreams.tck.*;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.Test;

/**
 * Base abstract class for Folyam verifications, contains support for creating
 * Iterable range of values.
 * 
 * @param <T> the element type
 */
@Test
public abstract class BaseTck<T> extends FlowPublisherVerification<T> {

    public BaseTck() {
        this(25L);
    }

    public BaseTck(long timeout) {
        super(new TestEnvironment(timeout));
    }

    /*
    @Override
    public Publisher<T> createFailedPublisher() {
        return bridge(Folyam.error(new IOException()));
    }

    @Override
    public final Publisher<T> createPublisher(long elements) {
        return bridge(createFlowPublisher(elements));
    }

    public abstract Flow.Publisher<T> createFlowPublisher(long elements);
    */

    @Override
    public Flow.Publisher<T> createFailedFlowPublisher() {
        return Folyam.error(new IOException());
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1024;
    }

    /**
     * Creates an Iterable with the specified number of elements or an infinite one if
     * elements > Integer.MAX_VALUE.
     * @param elements the number of elements to return, Integer.MAX_VALUE means an infinite sequence
     * @return the Iterable
     */
    protected Iterable<Long> iterate(long elements) {
        return iterate(elements > Integer.MAX_VALUE, elements);
    }

    protected Iterable<Long> iterate(boolean useInfinite, long elements) {
        return useInfinite ? new InfiniteRange() : new FiniteRange(elements);
    }

    /**
     * Create an array of Long values, ranging from 0L to elements - 1L.
     * @param elements the number of elements to return
     * @return the array
     */
    protected Long[] array(long elements) {
        Long[] a = new Long[(int)elements];
        for (int i = 0; i < elements; i++) {
            a[i] = (long)i;
        }
        return a;
    }

    static final class FiniteRange implements Iterable<Long> {
        final long end;
        FiniteRange(long end) {
            this.end = end;
        }

        @Override
        public Iterator<Long> iterator() {
            return new FiniteRangeIterator(end);
        }

        static final class FiniteRangeIterator implements Iterator<Long> {
            final long end;
            long count;

            FiniteRangeIterator(long end) {
                this.end = end;
            }

            @Override
            public boolean hasNext() {
                return count != end;
            }

            @Override
            public Long next() {
                long c = count;
                if (c != end) {
                    count = c + 1;
                    return c;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    static final class InfiniteRange implements Iterable<Long> {
        @Override
        public Iterator<Long> iterator() {
            return new InfiniteRangeIterator();
        }

        static final class InfiniteRangeIterator implements Iterator<Long> {
            long count;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Long next() {
                return count++;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }

    static <T> org.reactivestreams.Publisher<T> bridge(Flow.Publisher<T> actual) {
        return new FlowBridge<>(actual);
    }

    static final class FlowBridge<T> implements org.reactivestreams.Publisher<T> {

        final Flow.Publisher<T> actual;

        FlowBridge(Flow.Publisher<T> actual) {
            this.actual = actual;
        }

        @Override
        public void subscribe(Subscriber<? super T> s) {
            if (s == null) {
                actual.subscribe(null);
            } else {
                actual.subscribe(new FlowBridgeSubscriber<>(s));
            }
        }

        static final class FlowBridgeSubscriber<T> implements Flow.Subscriber<T>, org.reactivestreams.Subscription {

            final Subscriber<? super T> actual;

            Flow.Subscription upstream;

            FlowBridgeSubscriber(Subscriber<? super T> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription s) {
                upstream = s;
                actual.onSubscribe(this);
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public void request(long n) {
                upstream.request(n);
            }

            @Override
            public void cancel() {
                upstream.cancel();
            }
        }
    }
}
