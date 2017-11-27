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

package hu.akarnokd.reactive4javaflow.impl.schedulers;

import hu.akarnokd.reactive4javaflow.SchedulerService;

/**
 * Allows retrieving multiple workers from the implementing
 * {@link hu.akarnokd.reactive4javaflow.SchedulerService} in a way that when asking for
 * at most the parallelism level of the Scheduler, those
 * {@link hu.akarnokd.reactive4javaflow.SchedulerService.Worker} instances will be running
 * with different backing threads.
 *
 * @since 0.1.5
 */
public interface SchedulerMultiWorkerSupport {

    /**
     * Creates the given number of {@link hu.akarnokd.reactive4javaflow.SchedulerService.Worker} instances
     * that are possibly backed by distinct threads
     * and calls the specified {@code WorkerCallback} with them.
     * @param number the number of workers to create, positive
     * @param callback the callback to send worker instances to
     */
    void createWorkers(int number, WorkerCallback callback);

    /**
     * The callback interface for the {@link SchedulerMultiWorkerSupport#createWorkers(int, WorkerCallback)}
     * method.
     */
    interface WorkerCallback {
        /**
         * Called with the Worker index and instance.
         * @param index the worker index, zero-based
         * @param worker the worker instance
         */
        void onWorker(int index, SchedulerService.Worker worker);
    }
}
