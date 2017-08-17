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

import java.util.concurrent.Flow;

public final class EsetlegTimeoutSelectorFallback<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final Flow.Publisher<?> firstTimeout;

    final Esetleg<? extends T> fallback;

    public EsetlegTimeoutSelectorFallback(Esetleg<T> source, Flow.Publisher<?> firstTimeout, Esetleg<? extends T> fallback) {
        this.source = source;
        this.firstTimeout = firstTimeout;
        this.fallback = fallback;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FolyamTimeoutSelectorFallback.TimeoutTimedSelectorFallbackConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, firstTimeout, v -> Esetleg.never(), fallback));
        } else {
            source.subscribe(new FolyamTimeoutSelectorFallback.TimeoutTimedSelectorFallbackSubscriber<>(s, firstTimeout, v -> Esetleg.never(), fallback));
        }
    }
}
