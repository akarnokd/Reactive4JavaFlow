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

import java.util.concurrent.Flow;

public final class FolyamConcatArrayEager<T> extends Folyam<T> {

    final Flow.Publisher<? extends T>[] sources;

    final boolean delayError;

    public FolyamConcatArrayEager(Flow.Publisher<? extends T>[] sources, boolean delayError) {
        this.sources = sources;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super T> s) {
        Flow.Publisher<? extends T>[] srcs = this.sources;
        int n = srcs.length;

        FolyamSubscriber<? super Flow.Publisher<? extends T>> subscriber =
                FolyamConcatMapEager.createSubscriber(s, v -> v, Integer.MAX_VALUE, FolyamPlugins.defaultBufferSize(), delayError);
        subscriber.onSubscribe(new FolyamArray.ArraySubscription<>(subscriber, srcs, 0, n));
    }

    /*
    public static <T> Folyam<T> mergeWith(Folyam<T> source, Flow.Publisher<? extends T> other, boolean delayError) {
        if (source instanceof FolyamConcatArrayEager) {
            FolyamConcatArrayEager aa = (FolyamConcatArrayEager) source;
            if (aa.delayError == delayError) {
                int n = aa.sources.length;
                Flow.Publisher<? extends T>[] newSrcs = Arrays.copyOf(aa.sources, n + 1);
                newSrcs[n] = other;
                return new FolyamConcatArrayEager<>(newSrcs, delayError);
            }
        }
        return new FolyamConcatArrayEager<>(new Flow.Publisher[] { source, other }, delayError);

    }
    */
}
