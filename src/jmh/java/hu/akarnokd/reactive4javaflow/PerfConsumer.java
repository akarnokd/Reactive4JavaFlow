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

import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.Flow;

public final class PerfConsumer implements FolyamSubscriber<Object> {
    final Blackhole bh;

    public PerfConsumer(Blackhole bh) {
        this.bh = bh;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        bh.consume(subscription);
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(Object item) {
        bh.consume(item);
    }

    @Override
    public void onError(Throwable throwable) {
        bh.consume(throwable);
    }

    @Override
    public void onComplete() {
        bh.consume(true);
    }

}
