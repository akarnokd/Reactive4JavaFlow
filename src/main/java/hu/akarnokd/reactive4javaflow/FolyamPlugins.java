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
package hu.akarnokd.reactive4javaflow;

import java.util.concurrent.Flow;
import java.util.function.*;

public final class FolyamPlugins {

    static volatile Consumer<? super Throwable> onError;

    static volatile BiFunction<? super Folyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> folyamOnSubscribe;

    static volatile BiFunction<? super Esetleg, ? super FolyamSubscriber, ? extends FolyamSubscriber> esetlegOnSubscribe;

    static volatile BiFunction<? super ParallelFolyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> parallelOnSubscribe;

    static volatile Function<? super Folyam, ? extends Folyam> folyamOnAssembly;

    static volatile Function<? super Esetleg, ? extends Esetleg> esetlegOnAssembly;

    static volatile Function<? super ParallelFolyam, ? extends ParallelFolyam> parallelOnAssembly;

    static volatile Function<? super ConnectableFolyam, ? extends ConnectableFolyam> connectableOnAssembly;

    private FolyamPlugins() {
        throw new IllegalStateException("No instances!");
    }

    public static int defaultBufferSize() {
        return 128; // TODO make customizable
    }

    public static <T> FolyamSubscriber<? super T> onSubscribe(Folyam<T> parent, FolyamSubscriber<? super T> s) {
        return s; // TODO make customizable
    }

    public static <T> FolyamSubscriber<? super T> onSubscribe(Esetleg<T> parent, FolyamSubscriber<? super T> s) {
        return s; // TODO make customizable
    }

    public static <T> FolyamSubscriber<? super T> onSubscribe(ParallelFolyam<T> parent, FolyamSubscriber<? super T> s) {
        return s; // TODO make customizable
    }

    public static <T> Folyam<T> onAssembly(Folyam<T> upstream) {
        Function<? super Folyam, ? extends Folyam> h = folyamOnAssembly;
        if (h != null) {
            return h.apply(upstream);
        }
        return upstream;
    }

    public static <T> Esetleg<T> onAssembly(Esetleg<T> upstream) {
        Function<? super Esetleg, ? extends Esetleg> h = esetlegOnAssembly;
        if (h != null) {
            return h.apply(upstream);
        }
        return upstream;
    }

    public static <T> ParallelFolyam<T> onAssembly(ParallelFolyam<T> upstream) {
        Function<? super ParallelFolyam, ? extends ParallelFolyam> h = parallelOnAssembly;
        if (h != null) {
            return h.apply(upstream);
        }
        return upstream;
    }

    public static <T> ConnectableFolyam<T> onAssembly(ConnectableFolyam<T> upstream) {
        Function<? super ConnectableFolyam, ? extends ConnectableFolyam> h = connectableOnAssembly;
        if (h != null) {
            return h.apply(upstream);
        }
        return upstream;
    }

    public static void onError(Throwable ex) {
        Consumer<? super Throwable> h = onError;
        if (h != null) {
            try {
                h.accept(ex);
                return;
            } catch (Throwable exc) {
                exc.printStackTrace();
            }
        }
        ex.printStackTrace();
    }

    public static void setOnError(Consumer<? super Throwable> handler) {
        FolyamPlugins.onError = handler;
    }

    public static Consumer<? super Throwable> getOnError() {
        return onError;
    }

    public static void handleFatal(Throwable ex) {
        if (ex instanceof Error) {
            throw (Error)ex;
        }
    }
}
