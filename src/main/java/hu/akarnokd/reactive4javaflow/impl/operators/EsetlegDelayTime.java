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

import java.util.concurrent.TimeUnit;

public final class EsetlegDelayTime<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final long delay;

    final TimeUnit unit;

    final SchedulerService executor;

    public EsetlegDelayTime(Esetleg<T> source, long delay, TimeUnit unit, SchedulerService executor) {
        this.source = source;
        this.delay = delay;
        this.unit = unit;
        this.executor = executor;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        source.subscribe(new FolyamDelayTime.DelaySubscriber<>(s, delay, unit, executor.worker()));
    }
}
