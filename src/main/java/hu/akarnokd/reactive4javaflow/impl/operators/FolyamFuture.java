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

import java.util.concurrent.*;

public final class FolyamFuture<T> extends Folyam<T> implements FusedDynamicSource<T> {

    final Future<? extends T> future;

    final long timeout;

    final TimeUnit unit;

    public FolyamFuture(Future<? extends T> future, long timeout, TimeUnit unit) {
        this.future = future;
        this.timeout = timeout;
        this.unit = unit;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        DeferredScalarSubscription<T> dss = new DeferredScalarSubscription<>(s);
        s.onSubscribe(dss);

        if (!dss.isCancelled()) {
            T v;
            try {
                if (unit != null) {
                    v = future.get(timeout, unit);
                } else {
                    v = future.get();
                }
            } catch (ExecutionException ex) {
                dss.error(ex.getCause());
                return;
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
        try {
            if (unit == null) {
                return future.get();
            } else {
                return future.get(timeout, unit);
            }
        } catch (ExecutionException ex) {
            throw ex.getCause();
        }
    }
}
