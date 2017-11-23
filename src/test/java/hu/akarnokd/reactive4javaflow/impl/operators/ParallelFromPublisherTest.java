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

import static org.junit.Assert.*;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;
import hu.akarnokd.reactive4javaflow.impl.BooleanSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public class ParallelFromPublisherTest {

    @Test
    public void sourceOverflow() {
        new Folyam<Integer>() {
            @Override
            protected void subscribeActual(FolyamSubscriber<? super Integer> s) {
                s.onSubscribe(new BooleanSubscription());
                for (int i = 0; i < 10; i++) {
                    s.onNext(i);
                }
            }
        }
        .parallel(1, 1)
        .sequential(1)
        .test(0)
        .assertFailure(IllegalStateException.class);
    }

    @Test
    public void fusedFilterBecomesEmpty() {
        Folyam.just(1)
        .filter(v -> false)
        .parallel()
        .sequential()
        .test()
        .assertResult();
    }

    static final class StripBoundary<T> extends Folyam<T> implements FolyamTransformer<T, T> {

        final Folyam<T> source;

        StripBoundary(Folyam<T> source) {
            this.source = source;
        }

        @Override
        public Folyam<T> apply(Folyam<T> upstream) {
            return new StripBoundary<>(upstream);
        }

        @Override
        protected void subscribeActual(FolyamSubscriber<? super T> s) {
            source.subscribe(new StripBoundarySubscriber<>(s));
        }

        static final class StripBoundarySubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

            final FolyamSubscriber<? super T> actual;

            Flow.Subscription upstream;

            FusedSubscription<T> qs;

            StripBoundarySubscriber(FolyamSubscriber<? super T> actual) {
                this.actual = actual;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.upstream = subscription;
                if (subscription instanceof FusedSubscription) {
                    qs = (FusedSubscription<T>)subscription;
                }
                actual.onSubscribe(this);
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable throwable) {
                actual.onError(throwable);
            }

            @Override
            public void onComplete() {
                actual.onComplete();
            }

            @Override
            public int requestFusion(int mode) {
                FusedSubscription<T> fs = qs;
                if (fs != null) {
                    return fs.requestFusion(mode & ~FusedSubscription.BOUNDARY);
                }
                return FusedSubscription.NONE;
            }

            @Override
            public T poll() throws Throwable {
                return qs.poll();
            }

            @Override
            public void clear() {
                qs.clear();
            }

            @Override
            public boolean isEmpty() {
                return qs.isEmpty();
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

    @Test
    public void syncFusedMapCrash() {
        Folyam.just(1)
                .map(v -> {
                    throw new IOException();
                })
                .compose(new StripBoundary<>(null))
                .parallel()
                .sequential()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void asyncFusedMapCrash() {
        SolocastProcessor<Integer> up = new SolocastProcessor<>();

        up.onNext(1);

        up
                .map(v -> {
                    throw new IOException();
                })
                .compose(new StripBoundary<>(null))
                .parallel()
                .sequential()
                .test()
                .assertFailure(IOException.class);

        assertFalse(up.hasSubscribers());
    }

    @Test
    public void boundaryConfinement() {
        final Set<String> between = new HashSet<>();
        final ConcurrentHashMap<String, String> processing = new ConcurrentHashMap<>();

        Folyam.range(1, 10)
                .observeOn(SchedulerServices.single(), 1)
                .doOnNext(v -> between.add(Thread.currentThread().getName()))
                .parallel(2, 1)
                .runOn(SchedulerServices.computation(), 1)
                .map((CheckedFunction<Integer, Object>) v -> {
                    processing.putIfAbsent(Thread.currentThread().getName(), "");
                    return v;
                })
                .sequential()
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertValueSet(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .assertComplete()
                .assertNoErrors()
        ;

        assertEquals(between.toString(), 1, between.size());
        assertTrue(between.toString(), between.iterator().next().contains("Reactive4JavaFlow.Single"));

        for (String e : processing.keySet()) {
            assertTrue(processing.toString(), e.contains("Reactive4JavaFlow.CPU"));
        }
    }}
