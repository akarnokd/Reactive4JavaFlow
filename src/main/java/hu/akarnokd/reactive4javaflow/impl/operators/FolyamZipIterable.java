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
import hu.akarnokd.reactive4javaflow.impl.EmptySubscription;

import java.util.Arrays;
import java.util.concurrent.Flow;

public final class FolyamZipIterable<T, R> extends Folyam<R> {

    final Iterable<? extends Flow.Publisher<? extends T>> sources;

    final CheckedFunction<? super Object[], ? extends R> zipper;

    final int prefetch;

    final boolean delayError;

    public FolyamZipIterable(Iterable<? extends Flow.Publisher<? extends T>> sources, CheckedFunction<? super Object[], ? extends R> zipper, int prefetch, boolean delayError) {
        this.sources = sources;
        this.zipper = zipper;
        this.prefetch = prefetch;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        Flow.Publisher<? extends T>[] srcs = new Flow.Publisher[8];
        int n = 0;
        try {
            for (Flow.Publisher<? extends T> p : sources) {
                if (n == srcs.length) {
                    srcs = Arrays.copyOf(srcs, n + (n >> 2));
                }
                srcs[n++] = p;
            }
        } catch (Throwable ex) {
            FolyamPlugins.handleFatal(ex);
            EmptySubscription.error(s, ex);
            return;
        }
        FolyamZipArray.subscribe(srcs, n, s, zipper, prefetch, delayError);
    }
}
