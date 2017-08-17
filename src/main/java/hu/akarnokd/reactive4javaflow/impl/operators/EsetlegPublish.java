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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;
import hu.akarnokd.reactive4javaflow.processors.MulticastProcessor;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class EsetlegPublish<T, R> extends Esetleg<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super Folyam<T>, ? extends Esetleg<? extends R>> handler;

    public EsetlegPublish(Esetleg<T> source, CheckedFunction<? super Folyam<T>, ? extends Esetleg<? extends R>> handler) {
        this.source = source;
        this.handler = handler;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        MulticastProcessor<T> mp = new MulticastProcessor<>(1);
        Flow.Publisher<? extends R> p;
        try {
            p = Objects.requireNonNull(handler.apply(mp), "The handler returned a null Flow.Publisher");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            p.subscribe(new FolyamPublish.PublishConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, mp));
        } else {
            p.subscribe(new FolyamPublish.PublishSubscriber<>(s, mp));
        }

        source.subscribe(mp);
    }
}
