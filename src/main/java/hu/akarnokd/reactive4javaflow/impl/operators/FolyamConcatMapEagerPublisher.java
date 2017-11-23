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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

public final class FolyamConcatMapEagerPublisher<T, R> extends Folyam<R> {

    final Flow.Publisher<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    final int maxConcurrency;

    final int prefetch;

    final boolean delayError;

    public FolyamConcatMapEagerPublisher(Flow.Publisher<T> source,
                                         CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper,
                                         int maxConcurrency,
                                         int prefetch,
                                         boolean delayError) {
        this.source = source;
        this.mapper = mapper;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }


    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        source.subscribe(createSubscriber(s, mapper, maxConcurrency, prefetch, delayError));
    }

    public static <T, R> FolyamSubscriber<T> createSubscriber(FolyamSubscriber<? super R> s, CheckedFunction<? super T, ? extends Flow.Publisher<? extends R>> mapper, int maxConcurrency, int prefetch, boolean delayErrors) {
        if (s instanceof ConditionalSubscriber) {
            return new FolyamConcatMapEager.ConcatMapEagerConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mapper, maxConcurrency, prefetch, delayErrors);
        }
        return new FolyamConcatMapEager.ConcatMapEagerSubscriber<>(s, mapper, maxConcurrency, prefetch, delayErrors);
    }
}
