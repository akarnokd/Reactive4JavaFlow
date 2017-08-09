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

import java.util.concurrent.*;

public final class FolyamDelayTime<T> extends Folyam<T> {

    final Folyam<T> source;

    final long delay;

    final TimeUnit unit;

    final SchedulerService executor;

    public FolyamDelayTime(Folyam<T> source, long delay, TimeUnit unit, SchedulerService executor) {
        this.source = source;
        this.delay = delay;
        this.unit = unit;
        this.executor = executor;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new DelaySubscriber<>(s, delay, unit, executor.worker()));
    }

    static final class DelaySubscriber<T> implements FolyamSubscriber<T>, Flow.Subscription, Runnable {

        final FolyamSubscriber<? super T> actual;

        final SchedulerService.Worker worker;

        final long delay;

        final TimeUnit unit;

        Flow.Subscription upstream;

        Throwable error;

        DelaySubscriber(FolyamSubscriber<? super T> actual, long delay, TimeUnit unit, SchedulerService.Worker worker) {
            this.actual = actual;
            this.delay = delay;
            this.unit = unit;
            this.worker = worker;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstream = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T item) {
            worker.schedule(() -> actual.onNext(item), delay, unit);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            worker.schedule(this, delay, unit);
        }

        @Override
        public void onComplete() {
            worker.schedule(this, delay, unit);
        }

        @Override
        public void run() {
            Throwable ex = error;
            if (ex == null) {
                actual.onComplete();
            } else {
                actual.onError(ex);
            }
            worker.close();
        }

        @Override
        public void request(long n) {
            upstream.request(n);
        }

        @Override
        public void cancel() {
            upstream.cancel();
            worker.close();
        }
    }
}
