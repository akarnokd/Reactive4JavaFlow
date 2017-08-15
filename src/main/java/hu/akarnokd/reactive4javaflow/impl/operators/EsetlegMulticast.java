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
import hu.akarnokd.reactive4javaflow.impl.*;

import java.util.Objects;
import java.util.concurrent.Flow;

public final class EsetlegMulticast<T, U, R> extends Esetleg<R> {

    final Esetleg<T> source;

    final CheckedFunction<? super Esetleg<T>, ? extends ConnectableFolyam<U>> multicaster;

    final CheckedFunction<? super Folyam<U>, ? extends Esetleg<? extends R>> handler;

    public EsetlegMulticast(Esetleg<T> source, CheckedFunction<? super Esetleg<T>, ? extends ConnectableFolyam<U>> multicaster, CheckedFunction<? super Folyam<U>, ? extends Esetleg<? extends R>> handler) {
        this.source = source;
        this.multicaster = multicaster;
        this.handler = handler;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        Flow.Publisher<? extends R> p;
        ConnectableFolyam<U> conn;
        try {
            conn = Objects.requireNonNull(multicaster.apply(source), "The multicaster returned a null ConnectableFolyam");
            p = Objects.requireNonNull(handler.apply(conn), "The handler returned a null Flow.Publisher");
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        SequentialAutoDisposable sd = new SequentialAutoDisposable();

        if (s instanceof ConditionalSubscriber) {
            p.subscribe(new FolyamPublish.PublishConditionalSubscriber<>((ConditionalSubscriber<? super R>)s, sd));
        } else {
            p.subscribe(new FolyamPublish.PublishSubscriber<>(s, sd));
        }

        conn.connect(sd::replace);
    }
}
