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

    static volatile Function<Supplier<SchedulerService>, SchedulerService> onInitSingleSchedulerService;

    static volatile Function<Supplier<SchedulerService>, SchedulerService> onInitComputationSchedulerService;

    static volatile Function<Supplier<SchedulerService>, SchedulerService> onInitIOSchedulerService;

    static volatile Function<Supplier<SchedulerService>, SchedulerService> onInitNewThreadSchedulerService;

    static volatile Function<SchedulerService, SchedulerService> onSingleSchedulerService;

    static volatile Function<SchedulerService, SchedulerService> onComputationSchedulerService;

    static volatile Function<SchedulerService, SchedulerService> onIOSchedulerService;

    static volatile Function<SchedulerService, SchedulerService> onNewThreadSchedulerService;

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

    public static BiFunction<? super Folyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> getFolyamOnSubscribe() {
        return folyamOnSubscribe;
    }

    public static void setFolyamOnSubscribe(BiFunction<? super Folyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> folyamOnSubscribe) {
        FolyamPlugins.folyamOnSubscribe = folyamOnSubscribe;
    }

    public static BiFunction<? super Esetleg, ? super FolyamSubscriber, ? extends FolyamSubscriber> getEsetlegOnSubscribe() {
        return esetlegOnSubscribe;
    }

    public static void setEsetlegOnSubscribe(BiFunction<? super Esetleg, ? super FolyamSubscriber, ? extends FolyamSubscriber> esetlegOnSubscribe) {
        FolyamPlugins.esetlegOnSubscribe = esetlegOnSubscribe;
    }

    public static BiFunction<? super ParallelFolyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> getParallelOnSubscribe() {
        return parallelOnSubscribe;
    }

    public static void setParallelOnSubscribe(BiFunction<? super ParallelFolyam, ? super FolyamSubscriber, ? extends FolyamSubscriber> parallelOnSubscribe) {
        FolyamPlugins.parallelOnSubscribe = parallelOnSubscribe;
    }

    public static Function<? super Folyam, ? extends Folyam> getFolyamOnAssembly() {
        return folyamOnAssembly;
    }

    public static void setFolyamOnAssembly(Function<? super Folyam, ? extends Folyam> folyamOnAssembly) {
        FolyamPlugins.folyamOnAssembly = folyamOnAssembly;
    }

    public static Function<? super Esetleg, ? extends Esetleg> getEsetlegOnAssembly() {
        return esetlegOnAssembly;
    }

    public static void setEsetlegOnAssembly(Function<? super Esetleg, ? extends Esetleg> esetlegOnAssembly) {
        FolyamPlugins.esetlegOnAssembly = esetlegOnAssembly;
    }

    public static Function<? super ParallelFolyam, ? extends ParallelFolyam> getParallelOnAssembly() {
        return parallelOnAssembly;
    }

    public static void setParallelOnAssembly(Function<? super ParallelFolyam, ? extends ParallelFolyam> parallelOnAssembly) {
        FolyamPlugins.parallelOnAssembly = parallelOnAssembly;
    }

    public static Function<? super ConnectableFolyam, ? extends ConnectableFolyam> getConnectableOnAssembly() {
        return connectableOnAssembly;
    }

    public static void setConnectableOnAssembly(Function<? super ConnectableFolyam, ? extends ConnectableFolyam> connectableOnAssembly) {
        FolyamPlugins.connectableOnAssembly = connectableOnAssembly;
    }

    public static Function<Supplier<SchedulerService>, SchedulerService> getOnInitSingleSchedulerService() {
        return onInitSingleSchedulerService;
    }

    public static void setOnInitSingleSchedulerService(Function<Supplier<SchedulerService>, SchedulerService> onInitSingleSchedulerService) {
        FolyamPlugins.onInitSingleSchedulerService = onInitSingleSchedulerService;
    }

    public static Function<Supplier<SchedulerService>, SchedulerService> getOnInitComputationSchedulerService() {
        return onInitComputationSchedulerService;
    }

    public static void setOnInitComputationSchedulerService(Function<Supplier<SchedulerService>, SchedulerService> onInitComputationSchedulerService) {
        FolyamPlugins.onInitComputationSchedulerService = onInitComputationSchedulerService;
    }

    public static Function<Supplier<SchedulerService>, SchedulerService> getOnInitIOSchedulerService() {
        return onInitIOSchedulerService;
    }

    public static void setOnInitIOSchedulerService(Function<Supplier<SchedulerService>, SchedulerService> onInitIOSchedulerService) {
        FolyamPlugins.onInitIOSchedulerService = onInitIOSchedulerService;
    }

    public static Function<Supplier<SchedulerService>, SchedulerService> getOnInitNewThreadSchedulerService() {
        return onInitNewThreadSchedulerService;
    }

    public static void setOnInitNewThreadSchedulerService(Function<Supplier<SchedulerService>, SchedulerService> onInitNewThreadSchedulerService) {
        FolyamPlugins.onInitNewThreadSchedulerService = onInitNewThreadSchedulerService;
    }

    public static Function<SchedulerService, SchedulerService> getOnSingleSchedulerService() {
        return onSingleSchedulerService;
    }

    public static void setOnSingleSchedulerService(Function<SchedulerService, SchedulerService> onSingleSchedulerService) {
        FolyamPlugins.onSingleSchedulerService = onSingleSchedulerService;
    }

    public static Function<SchedulerService, SchedulerService> getOnComputationSchedulerService() {
        return onComputationSchedulerService;
    }

    public static void setOnComputationSchedulerService(Function<SchedulerService, SchedulerService> onComputationSchedulerService) {
        FolyamPlugins.onComputationSchedulerService = onComputationSchedulerService;
    }

    public static Function<SchedulerService, SchedulerService> getOnIOSchedulerService() {
        return onIOSchedulerService;
    }

    public static void setOnIOSchedulerService(Function<SchedulerService, SchedulerService> onIOSchedulerService) {
        FolyamPlugins.onIOSchedulerService = onIOSchedulerService;
    }

    public static Function<SchedulerService, SchedulerService> getOnNewThreadSchedulerService() {
        return onNewThreadSchedulerService;
    }

    public static void setOnNewThreadSchedulerService(Function<SchedulerService, SchedulerService> onNewThreadSchedulerService) {
        FolyamPlugins.onNewThreadSchedulerService = onNewThreadSchedulerService;
    }
}
