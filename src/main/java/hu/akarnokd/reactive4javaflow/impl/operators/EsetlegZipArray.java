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

import java.util.concurrent.Flow;

public final class EsetlegZipArray<T, R> extends Esetleg<R> {

    final Esetleg<? extends T>[] sources;

    final CheckedFunction<? super Object[], ? extends R> zipper;

    final boolean delayError;

    public EsetlegZipArray(Esetleg<? extends T>[] sources, CheckedFunction<? super Object[], ? extends R> zipper, boolean delayError) {
        this.sources = sources;
        this.zipper = zipper;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super R> s) {
        Flow.Publisher<? extends T>[] srcs = this.sources;
        int n = srcs.length;
        subscribe(srcs, n, s, zipper, delayError);
    }

    public static <T, R> void subscribe(Flow.Publisher<? extends T>[] srcs, int n, FolyamSubscriber<? super R> actual, CheckedFunction<? super Object[], ? extends R> zipper, boolean delayError) {

        if (n == 0) {
            EmptySubscription.complete(actual);
            return;
        }
        if (actual instanceof ConditionalSubscriber) {
            if (n == 1) {
                srcs[0].subscribe(new FolyamMap.MapConditionalSubscriber<T, R>((ConditionalSubscriber<? super R>) actual, v -> zipper.apply(new Object[] { v })));
            } else {
                FolyamZipArray.ZipArrayConditionalCoordinator<T, R> parent = new FolyamZipArray.ZipArrayConditionalCoordinator<>((ConditionalSubscriber<? super R>) actual, zipper, n, 1, delayError);
                actual.onSubscribe(parent);
                parent.subscribe(srcs, n);
            }
        } else {
            if (n == 1) {
                srcs[0].subscribe(new FolyamMap.MapSubscriber<>(actual, v -> zipper.apply(new Object[]{ v })));
            } else {
                FolyamZipArray.ZipArrayCoordinator<T, R> parent = new FolyamZipArray.ZipArrayCoordinator<>(actual, zipper, n, 1, delayError);
                actual.onSubscribe(parent);
                parent.subscribe(srcs, n);
            }
        }
    }
}
