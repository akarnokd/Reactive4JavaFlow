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

public final class FolyamPlugins {

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
        return upstream; // TODO make customizable
    }

    public static <T> Esetleg<T> onAssembly(Esetleg<T> upstream) {
        return upstream; // TODO make customizable
    }

    public static <T> ParallelFolyam<T> onAssembly(ParallelFolyam<T> upstream) {
        return upstream; // TODO make customizable
    }

    public static void onError(Throwable ex) {
        // TODO implement
        ex.printStackTrace();
    }

    public static void handleFatal(Throwable ex) {
        if (ex instanceof Error) {
            throw (Error)ex;
        }
    }
}
