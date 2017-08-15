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
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;
import hu.akarnokd.reactive4javaflow.impl.util.*;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class EsetlegObserveOn<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final SchedulerService executor;

    public EsetlegObserveOn(Esetleg<T> source, SchedulerService executor) {
        this.source = source;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FolyamObserveOn.ObserveOnConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, 1, executor.worker()));
        } else {
            source.subscribe(new FolyamObserveOn.ObserveOnSubscriber<>(s, 1, executor.worker()));
        }
    }
}
