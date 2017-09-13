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

/**
 * Hooks the onAssembly calls, times and counts the various method calls passing through it.
 * ONLY FOR SYNCHRONOUS STREAMS!
 */
@SuppressWarnings("rawtypes")
public class FolyamSynchronousProfiler {

    public final Map<String, CallStatistics> entries;

    public FolyamSynchronousProfiler() {
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

    public static final class CallStatistics {
        public String key;
        public long subscribeTime;
        public long subscribeCount;
        public long onSubscribeTime;
        public long onSubscribeCount;
        public long onNextTime;
        public long onNextCount;
        public long tryOnNextTime;
        public long tryOnNextCount;
        public long onErrorTime;
        public long onErrorCount;
        public long onCompleteTime;
        public long onCompleteCount;
        public long pollTime;
        public long pollCount;
        public long requestTime;
        public long requestCount;

        public long sumTime() {
            return subscribeTime + onSubscribeTime + tryOnNextTime
                    + onNextTime + onErrorTime + onCompleteTime + pollTime + requestTime;
        }

        @Override
        public String toString() {
            return key + "\t"
                    + subscribeTime + "\t" + subscribeCount + "\t" + div(subscribeTime, subscribeCount)
                    + onSubscribeTime + "\t" + onSubscribeCount + "\t" + div(onSubscribeTime, onSubscribeCount)
                    + onNextTime + "\t" + onNextCount + "\t" + div(onNextTime, onNextCount)
                    + tryOnNextTime + "\t" + tryOnNextCount + "\t" + div(tryOnNextTime, tryOnNextCount)
                    + onErrorTime + "\t" + onErrorCount + "\t" + div(onErrorTime, onErrorCount)
                    + onCompleteTime + "\t" + onCompleteCount + "\t" + div(onCompleteTime, onCompleteCount)
                    + pollTime + "\t" + pollCount + "\t" + div(pollTime, pollCount)
                    + requestTime + "\t" + requestCount + "\t" + div(requestTime, requestCount)
                    ;
        }

        String tf(long time, long count) {
            return String.format("     time = %10d ns, count = %7d, cost = %9d ns/call\r\n", time, count, count != 0 ? time / count : -1L);
        }

        public String print() {
            return key + "\r\n"
                + "    subscribe()  " + tf(subscribeTime, subscribeCount)
                + "    onSubscribe()" + tf(onSubscribeTime, onSubscribeCount)
                + "    onNext()     " + tf(onNextTime, onNextCount)
                + "    tryOnNext()  " + tf(tryOnNextTime, tryOnNextCount)
                + "    onError()    " + tf(onErrorTime, onErrorCount)
                + "    onComplete() " + tf(onCompleteTime, onCompleteCount)
                + "    poll()       " + tf(pollTime, pollCount)
                + "    request()    " + tf(requestTime, requestCount)
                ;
        }

        String div(long a, long b) {
            if (b != 0L) {
                return "" + (a / b);
            }
            return "~";
        }
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

                long now = System.nanoTime();

                actual.onSubscribe(this);

                long after = System.nanoTime();
                calls.onSubscribeCount++;
                calls.onSubscribeTime += Math.max(0, after - now);
            }

            @Override
            public void onNext(T t) {
                long now = System.nanoTime();

                actual.onNext(t);

                long after = System.nanoTime();
                calls.onNextCount++;
                calls.onNextTime += Math.max(0, after - now);
            }

            @Override
            public void onError(Throwable t) {
                long now = System.nanoTime();

                actual.onError(t);

                long after = System.nanoTime();
                calls.onErrorCount++;
                calls.onErrorTime += Math.max(0, after - now);
            }

            @Override
            public void onComplete() {
                long now = System.nanoTime();

                actual.onComplete();

                long after = System.nanoTime();
                calls.onCompleteCount++;
                calls.onCompleteTime += Math.max(0, after - now);
            }

            @Override
            public T poll() throws Throwable {
                long now = System.nanoTime();

                T v = qs.poll();

                long after = System.nanoTime();
                calls.pollCount++;
                calls.pollTime += Math.max(0, after - now);

                return v;
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
                long now = System.nanoTime();

                s.request(n);

                long after = System.nanoTime();
                calls.requestCount++;
                calls.requestTime += Math.max(0, after - now);
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

                long now = System.nanoTime();

                actual.onSubscribe(this);

                long after = System.nanoTime();
                calls.onSubscribeCount++;
                calls.onSubscribeTime += Math.max(0, after - now);
            }

            @Override
            public void onNext(T t) {
                long now = System.nanoTime();

                actual.onNext(t);

                long after = System.nanoTime();
                calls.onNextCount++;
                calls.onNextTime += Math.max(0, after - now);
            }

            @Override
            public boolean tryOnNext(T t) {
                long now = System.nanoTime();

                boolean b = actual.tryOnNext(t);

                long after = System.nanoTime();
                calls.onNextCount++;
                calls.onNextTime += Math.max(0, after - now);

                return b;
            }

            @Override
            public void onError(Throwable t) {
                long now = System.nanoTime();

                actual.onError(t);

                long after = System.nanoTime();
                calls.onErrorCount++;
                calls.onErrorTime += Math.max(0, after - now);
            }

            @Override
            public void onComplete() {
                long now = System.nanoTime();

                actual.onComplete();

                long after = System.nanoTime();
                calls.onCompleteCount++;
                calls.onCompleteTime += Math.max(0, after - now);
            }

            @Override
            public T poll() throws Throwable {
                long now = System.nanoTime();

                T v = qs.poll();

                long after = System.nanoTime();
                calls.pollCount++;
                calls.pollTime += Math.max(0, after - now);

                return v;
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
                long now = System.nanoTime();

                s.request(n);

                long after = System.nanoTime();
                calls.requestCount++;
                calls.requestTime += Math.max(0, after - now);
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
