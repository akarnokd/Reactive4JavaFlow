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
import hu.akarnokd.reactive4javaflow.impl.SubscriptionArbiter;

import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.Flow;

import static java.lang.invoke.MethodHandles.lookup;

public final class EsetlegSwitchIfEmptyMany<T> extends Esetleg<T> {

    final Esetleg<T> source;

    final Iterable<? extends Esetleg<? extends T>> others;

    public EsetlegSwitchIfEmptyMany(Esetleg<T> source, Iterable<? extends Esetleg<? extends T>> others) {
        this.source = source;
        this.others = others;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            FolyamSwitchIfEmptyMany.SwitchIfEmptyManyConditionalSubscriber<T> parent = new FolyamSwitchIfEmptyMany.SwitchIfEmptyManyConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, others);
            s.onSubscribe(parent);
            parent.subscribeNext(source);
        } else {
            FolyamSwitchIfEmptyMany.SwitchIfEmptyManySubscriber<T> parent = new FolyamSwitchIfEmptyMany.SwitchIfEmptyManySubscriber<>(s, others);
            s.onSubscribe(parent);
            parent.subscribeNext(source);
        }
    }
}
