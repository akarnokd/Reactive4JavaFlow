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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Arrays;
import java.util.concurrent.Flow;

public final class EsetlegAmbArray<T> extends Esetleg<T> {

    final Esetleg<? extends T>[] sources;

    public EsetlegAmbArray(Esetleg<? extends T>[] sources) {
        this.sources = sources;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Flow.Publisher<? extends T>[] srcs = sources;
        int n = srcs.length;
        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            Flow.Publisher<? extends T> fp = srcs[0];
            if (fp == null) {
                EmptySubscription.error(s, new NullPointerException("Flow.Publisher[0] == null"));
            } else {
                fp.subscribe(s);
            }
            return;
        }
        if (s instanceof ConditionalSubscriber) {
            FolyamAmbArray.AmbConditionalCoordinator<T> parent = new FolyamAmbArray.AmbConditionalCoordinator<>((ConditionalSubscriber<? super T>)s, n);
            s.onSubscribe(parent);
            parent.subscribe(srcs, n);
        } else {
            FolyamAmbArray.AmbCoordinator<T> parent = new FolyamAmbArray.AmbCoordinator<>(s, n);
            s.onSubscribe(parent);
            parent.subscribe(srcs, n);
        }
    }

    public static <T> Esetleg<T> ambWith(Esetleg<T> source, Esetleg<? extends T> other) {
        if (source instanceof EsetlegAmbArray) {
            EsetlegAmbArray aa = (EsetlegAmbArray) source;
            int n = aa.sources.length;
            Esetleg<? extends T>[] newSrcs = Arrays.copyOf(aa.sources, n + 1);
            newSrcs[n] = other;
            return new EsetlegAmbArray<>(newSrcs);
        }
        return new EsetlegAmbArray<>(new Esetleg[] { source, other });
    }
}
