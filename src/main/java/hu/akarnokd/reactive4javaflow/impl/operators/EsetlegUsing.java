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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.fused.*;
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Objects;
import java.util.concurrent.*;

public final class EsetlegUsing<T, R> extends Esetleg<T> {

    final Callable<R> resourceSupplier;

    final CheckedFunction<? super R, ? extends Esetleg<? extends T>> flowSupplier;

    final CheckedConsumer<? super R> resourceCleanup;

    final boolean eager;

    public EsetlegUsing(Callable<R> resourceSupplier, CheckedFunction<? super R, ? extends Esetleg<? extends T>> flowSupplier, CheckedConsumer<? super R> resourceCleanup, boolean eager) {
        this.resourceSupplier = resourceSupplier;
        this.flowSupplier = flowSupplier;
        this.resourceCleanup = resourceCleanup;
        this.eager = eager;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        R res;

        try {
            res = resourceSupplier.call();
        } catch (Throwable ex) {
            EmptySubscription.error(s, ex);
            return;
        }

        Esetleg<? extends T> p;

        try {
            p = Objects.requireNonNull(flowSupplier.apply(res), "The flowSupplier returned a null Flow.Publisher");
        } catch (Throwable ex) {
            if (eager) {
                try {
                    resourceCleanup.accept(res);
                } catch (Throwable exc) {
                    ex = new CompositeThrowable(ex, exc);
                }
                EmptySubscription.error(s, ex);
            } else {
                EmptySubscription.error(s, ex);
                try {
                    resourceCleanup.accept(res);
                } catch (Throwable exc) {
                    FolyamPlugins.onError(exc);
                }
            }
            return;
        }

        if (s instanceof ConditionalSubscriber) {
            p.subscribe(new FolyamUsing.UsingConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, res, resourceCleanup, eager));
        } else {
            p.subscribe(new FolyamUsing.UsingSubscriber<>(s, res, resourceCleanup, eager));
        }
    }
}
