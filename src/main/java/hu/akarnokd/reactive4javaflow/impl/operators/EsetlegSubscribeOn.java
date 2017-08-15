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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;
import hu.akarnokd.reactive4javaflow.impl.SubscriptionHelper;

import java.lang.invoke.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

public final class EsetlegSubscribeOn<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final SchedulerService executor;

    final boolean requestOn;

    public EsetlegSubscribeOn(Esetleg<T> source, SchedulerService executor, boolean requestOn) {
        this.source = source;
        this.executor = executor;
        this.requestOn = requestOn;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {

        SchedulerService.Worker w = executor.worker();

        FolyamSubscribeOn.AbstractSubscribeOn<T> parent;

        if (s instanceof ConditionalSubscriber) {
            parent = new FolyamSubscribeOn.SubscribeOnConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, w, !requestOn, source);
        } else {
            parent = new FolyamSubscribeOn.SubscribeOnSubscriber<>(s, w, !requestOn, source);
        }

        s.onSubscribe(parent);

        w.schedule(parent);
    }
}
