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
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import hu.akarnokd.reactive4javaflow.impl.DeferredScalarSubscription;

import java.util.concurrent.Callable;

public final class EsetlegCallableAllowEmpty<T> extends Esetleg<T> implements FusedDynamicSource<T> {

    final Callable<? extends T> callable;

    public EsetlegCallableAllowEmpty(Callable<? extends T> callable) {
        this.callable = callable;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        DeferredScalarSubscription<T> dss = new DeferredScalarSubscription<>(s);
        s.onSubscribe(dss);

        if (!dss.isCancelled()) {
            T v;
            try {
                v = callable.call();
            } catch (Throwable ex) {
                dss.error(ex);
                return;
            }
            if (v != null) {
                dss.complete(v);
            } else {
                dss.complete();
            }
        }
    }

    @Override
    public T value() throws Throwable {
        return callable.call();
    }
}
