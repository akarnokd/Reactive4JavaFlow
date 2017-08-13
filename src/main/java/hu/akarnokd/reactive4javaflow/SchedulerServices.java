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

import hu.akarnokd.reactive4javaflow.impl.schedulers.*;
import javafx.concurrent.ScheduledService;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.*;

public final class SchedulerServices {

    private SchedulerServices() {
        throw new IllegalStateException("No instances!");
    }

    static final SchedulerService SINGLE;

    static final SchedulerService COMPUTATION;

    static final SchedulerService IO;

    static final SchedulerService NEW_THREAD;

    static final SchedulerService TRAMPOLINE;

    static final class SingleHolder {
        static final SchedulerService INSTANCE = new SingleSchedulerService("Reactive4JavaFlow.Single", Thread.NORM_PRIORITY, true);
    }

    static final class ComputationHolder {
        static final SchedulerService INSTANCE = new ParallelSchedulerService(Runtime.getRuntime().availableProcessors(), "Reactive4JavaFlow.CPU", Thread.NORM_PRIORITY, true);
    }

    static final class IOHolder {
        static final SchedulerService INSTANCE = SingleHolder.INSTANCE; // FIXME implement
    }

    static final class NewThreadHolder {
        static final SchedulerService INSTANCE = new NewThreadSchedulerService("Reactive4JavaFlow.NewThread", Thread.NORM_PRIORITY, true);
    }

    static {

        TRAMPOLINE = new TrampolineSchedulerService();

        Function<Supplier<SchedulerService>, SchedulerService> init;
        SchedulerService scheduler;

        init = FolyamPlugins.onInitSingleSchedulerService;
        if (init == null) {
            init = Supplier::get;
        }
        SINGLE = Objects.requireNonNull(init.apply(() -> SingleHolder.INSTANCE), "Single SchedulerService initialized to null");

        init = FolyamPlugins.onInitComputationSchedulerService;
        if (init == null) {
            init = Supplier::get;
        }
        COMPUTATION = Objects.requireNonNull(init.apply(() -> ComputationHolder.INSTANCE), "Computation SchedulerService initialized to null");

        init = FolyamPlugins.onInitIOSchedulerService;
        if (init == null) {
            init = Supplier::get;
        }
        IO = Objects.requireNonNull(init.apply(() -> IOHolder.INSTANCE), "IO SchedulerService initialized to null");

        init = FolyamPlugins.onInitNewThreadSchedulerService;
        if (init == null) {
            init = Supplier::get;
        }
        NEW_THREAD = Objects.requireNonNull(init.apply(() -> NewThreadHolder.INSTANCE), "NewThread SchedulerService initialized to null");
    }

    public static SchedulerService single() {
        Function<SchedulerService, SchedulerService> f = FolyamPlugins.onSingleSchedulerService;
        return f != null ? f.apply(SINGLE) : SINGLE;
    }

    public static SchedulerService computation() {
        Function<SchedulerService, SchedulerService> f = FolyamPlugins.onComputationSchedulerService;
        return f != null ? f.apply(COMPUTATION) : COMPUTATION;
    }

    public static SchedulerService io() {
        Function<SchedulerService, SchedulerService> f = FolyamPlugins.onIOSchedulerService;
        return f != null ? f.apply(IO) : IO;
    }

    public static SchedulerService newThread() {
        Function<SchedulerService, SchedulerService> f = FolyamPlugins.onNewThreadSchedulerService;
        return f != null ? f.apply(NEW_THREAD) : NEW_THREAD;
    }

    public static SchedulerService trampoline() {
        return TRAMPOLINE;
    }

    public static void start() {
        SINGLE.start();
        COMPUTATION.start();
        IO.start();
        NEW_THREAD.start();
        ExecutorSchedulerService.startTimedHelpers();
    }

    public static void shutdown() {
        SINGLE.shutdown();
        COMPUTATION.shutdown();
        IO.start();
        NEW_THREAD.shutdown();
        ExecutorSchedulerService.shutdownTimedHelpers();
    }

    public static SchedulerService newSingle(String name) {
        return newSingle(name, Thread.NORM_PRIORITY, true);
    }

    public static SchedulerService newSingle(String name, int priority, boolean daemon) {
        return new SingleSchedulerService(name, priority, daemon);
    }

    public static SchedulerService newParallel(int parallelism, String name) {
        return newParallel(parallelism, name, Thread.NORM_PRIORITY, true);
    }

    public static SchedulerService newParallel(int parallelism, String name, int priority, boolean daemon) {
        return new ParallelSchedulerService(parallelism, name, priority, daemon);
    }

    public static SchedulerService newIO(String name) {
        return newIO(name, Thread.NORM_PRIORITY, true);
    }

    public static SchedulerService newIO(String name, int priority, boolean daemon) {
        // TODO implement
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    public static SchedulerService newThread(String name) {
        return newThread(name, Thread.NORM_PRIORITY, true);
    }

    public static SchedulerService newThread(String name, int priority, boolean daemon) {
        return new NewThreadSchedulerService(name, priority, daemon);
    }

    public static SchedulerService newShared(SchedulerService.Worker worker) {
        Objects.requireNonNull(worker, "worker == null");
        return new SharedSchedulerService(worker);
    }

    public static SchedulerService newExecutor(Executor exec) {
        return newExecutor(exec, true);
    }

    public static SchedulerService newExecutor(Executor exec, boolean trampoline) {
        Objects.requireNonNull(exec, "exec == null");
        return new ExecutorSchedulerService(exec, trampoline);
    }

    public static BlockingSchedulerService newBlocking() {
         return new BlockingSchedulerService();
    }

}
