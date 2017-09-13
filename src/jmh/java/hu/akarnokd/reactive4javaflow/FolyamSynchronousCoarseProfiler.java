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

package hu.akarnokd.reactive4javaflow;

import hu.akarnokd.reactive4javaflow.fused.*;

import java.util.*;
import java.util.concurrent.Flow;
import hu.akarnokd.reactive4javaflow.FolyamSynchronousProfiler.CallStatistics;

/**
 * Hooks the onAssembly calls, times and counts the various method calls passing through it.
 * ONLY FOR SYNCHRONOUS STREAMS!
 */
@SuppressWarnings("rawtypes")
public class FolyamSynchronousCoarseProfiler {

    public final Map<String, CallStatistics> entries;

    public FolyamSynchronousCoarseProfiler() {
        entries = new HashMap<>();
    }

    public void start() {
        FolyamPlugins.setFolyamOnAssembly(t -> {
            FolyamProfiler p = new FolyamProfiler(t);
            CallStatistics cs = new CallStatistics();
            cs.key = t.getClass().getSimpleName();
            CallStatistics cs2 = entries.putIfAbsent(cs.key, cs);

            if (cs2 == null) {
                p.stats = cs;
            } else {
                p.stats = cs2;
            }

            return p;
        });
        FolyamPlugins.setEsetlegOnAssembly(t -> {
            EsetlegProfiler p = new EsetlegProfiler(t);
            CallStatistics cs = new CallStatistics();
            cs.key = t.getClass().getSimpleName();
            CallStatistics cs2 = entries.putIfAbsent(cs.key, cs);

            if (cs2 == null) {
                p.stats = cs;
            } else {
                p.stats = cs2;
            }

            return p;
        });
    }

    public void stop() {
        FolyamPlugins.setFolyamOnAssembly(null);
        FolyamPlugins.setEsetlegOnAssembly(null);
    }

    public void clear() {
        entries.clear();
    }

    public void print() {
        List<CallStatistics> list = new ArrayList<>(entries.values());

        list.sort(Comparator.comparing(CallStatistics::sumTime).reversed());

        list.forEach(v -> System.out.println(v.print()));
    }

    static final class FolyamProfiler<T> extends Folyam<T> {

        final Folyam<T> source;

        CallStatistics stats;

        FolyamProfiler(Folyam<T> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(FolyamSubscriber<? super T> s) {
            long now = System.nanoTime();

            if (s instanceof ConditionalSubscriber) {
                source.subscribe(new ProfilerConditionalSubscriber<T>((ConditionalSubscriber<? super T>)s, stats));
            } else {
                source.subscribe(new ProfilerSubscriber<T>(s, stats));
            }

            long after = System.nanoTime();
            stats.subscribeCount++;
            stats.subscribeTime += Math.max(0, after - now);
        }

        static final class ProfilerSubscriber<T> implements FolyamSubscriber<T>, FusedSubscription<T> {

            final FolyamSubscriber<? super T> actual;

            final CallStatistics calls;

            Flow.Subscription s;

            FusedSubscription<T> qs;

            long startTime;

            ProfilerSubscriber(FolyamSubscriber<? super T> actual, CallStatistics calls) {
                this.actual = actual;
                this.calls = calls;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Flow.Subscription s) {
                this.s = s;
                if (s instanceof FusedSubscription) {
                    qs = (FusedSubscription<T>)s;
                }

                calls.onSubscribeCount++;
                startTime = System.nanoTime();

                actual.onSubscribe(this);
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);

                long after = System.nanoTime();
                calls.onErrorCount++;
                calls.onErrorTime += Math.max(0, after - startTime);
            }

            @Override
            public void onComplete() {
                actual.onComplete();

                long after = System.nanoTime();
                calls.onCompleteCount++;
                calls.onCompleteTime += Math.max(0, after - startTime);
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
                s.request(n);
            }

            @Override
            public int requestFusion(int mode) {
                FusedSubscription<T> qs = this.qs;
                return qs != null ? qs.requestFusion(mode) : NONE;
            }

            @Override
            public void cancel() {
                s.cancel();
            }
        }

        static final class ProfilerConditionalSubscriber<T> implements ConditionalSubscriber<T>, FusedSubscription<T> {

            final ConditionalSubscriber<? super T> actual;

            final CallStatistics calls;

            Flow.Subscription s;

            FusedSubscription<T> qs;

            long startTime;

            ProfilerConditionalSubscriber(ConditionalSubscriber<? super T> actual, CallStatistics calls) {
                this.actual = actual;
                this.calls = calls;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void onSubscribe(Flow.Subscription s) {
                this.s = s;
                if (s instanceof FusedSubscription) {
                    qs = (FusedSubscription<T>)s;
                }

                calls.onSubscribeCount++;
                startTime = System.nanoTime();

                actual.onSubscribe(this);
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public boolean tryOnNext(T t) {
                return actual.tryOnNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);

                long after = System.nanoTime();
                calls.onErrorCount++;
                calls.onErrorTime += Math.max(0, after - startTime);
            }

            @Override
            public void onComplete() {
                actual.onComplete();

                long after = System.nanoTime();
                calls.onCompleteCount++;
                calls.onCompleteTime += Math.max(0, after - startTime);
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
                s.request(n);
            }

            @Override
            public int requestFusion(int mode) {
                FusedSubscription<T> qs = this.qs;
                return qs != null ? qs.requestFusion(mode) : NONE;
            }

            @Override
            public void cancel() {
                s.cancel();
            }
        }
    }

    static final class EsetlegProfiler<T> extends Esetleg<T> {

        final Esetleg<T> source;

        CallStatistics stats;

        EsetlegProfiler(Esetleg<T> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(FolyamSubscriber<? super T> s) {
            long now = System.nanoTime();

            if (s instanceof ConditionalSubscriber) {
                source.subscribe(new FolyamProfiler.ProfilerConditionalSubscriber<T>((ConditionalSubscriber<? super T>) s, stats));
            } else {
                source.subscribe(new FolyamProfiler.ProfilerSubscriber<T>(s, stats));
            }

            long after = System.nanoTime();
            stats.subscribeCount++;
            stats.subscribeTime += Math.max(0, after - now);
        }
    }
}
