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
import hu.akarnokd.reactive4javaflow.fused.ConditionalSubscriber;

import java.util.concurrent.Flow;

public final class EsetlegFilterWhen<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter;

    final boolean delayError;

    public EsetlegFilterWhen(Esetleg<T> source, CheckedFunction<? super T, ? extends Flow.Publisher<Boolean>> filter, boolean delayError) {
        this.source = source;
        this.filter = filter;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new FolyamFilterWhen.FilterWhenConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, filter, 1, delayError));
        } else {
            source.subscribe(new FolyamFilterWhen.FilterWhenSubscriber<>(s, filter, 1, delayError));
        }
    }
}
