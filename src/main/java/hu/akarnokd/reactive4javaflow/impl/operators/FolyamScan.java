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
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.concurrent.Flow;

public final class FolyamScan<T> extends Folyam<T> {

    final Folyam<T> source;

    final CheckedBiFunction<T, T, T> scanner;

    public FolyamScan(Folyam<T> source, CheckedBiFunction<T, T, T> scanner) {
        this.source = source;
        this.scanner = scanner;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new ScanConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, scanner));
        } else {
            source.subscribe(new ScanSubscriber<>(s, scanner));
        }
    }

    static final class ScanSubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription {

        final FolyamSubscriber<? super T> actual;

        final CheckedBiFunction<T, T, T> scanner;

        Flow.Subscription upstream;

        T accumulator;

        boolean done;

        ScanSubscriber(FolyamSubscriber<? super T> actual, CheckedBiFunction<T, T, T> scanner) {
            this.actual = actual;
            this.scanner = scanner;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (done) {
                return;
            }
            T a = accumulator;
            if (a == null) {
                accumulator = item;
                actual.onNext(item);
            } else {
                try {
                    a = scanner.apply(a, item);
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return;
                }
                accumulator = a;
                actual.onNext(a);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            accumulator = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            accumulator = null;
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

    static final class ScanConditionalSubscriber<T> implements ConditionalSubscriber<T>, Flow.Subscription {

        final ConditionalSubscriber<? super T> actual;

        final CheckedBiFunction<T, T, T> scanner;

        Flow.Subscription upstream;

        T accumulator;

        boolean done;

        ScanConditionalSubscriber(ConditionalSubscriber<? super T> actual, CheckedBiFunction<T, T, T> scanner) {
            this.actual = actual;
            this.scanner = scanner;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            if (!tryOnNext(item) && !done) {
                upstream.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T item) {
            if (done) {
                return false;
            }
            T a = accumulator;
            if (a == null) {
                accumulator = item;
                return actual.tryOnNext(item);
            } else {
                try {
                    a = scanner.apply(a, item);
                } catch (Throwable ex) {
                    upstream.cancel();
                    onError(ex);
                    return false;
                }
                accumulator = a;
                return actual.tryOnNext(a);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (done) {
                FolyamPlugins.onError(throwable);
                return;
            }
            done = true;
            accumulator = null;
            actual.onError(throwable);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            accumulator = null;
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
