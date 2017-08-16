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

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.PeriodicTask;

import java.util.Objects;
import java.util.concurrent.*;


public interface SchedulerService {

    default AutoDisposable schedule(Runnable task) {
        return schedule(task, 0L, TimeUnit.NANOSECONDS);
    }

    default AutoDisposable schedule(Runnable task, long delay, TimeUnit unit) {
        Worker w = worker();
        AutoDisposable d = w.schedule(() -> {
            try {
                task.run();
            } catch (Throwable ex) {
                FolyamPlugins.onError(ex);
            } finally {
                w.close();
            }
        }, delay, unit);
        if (d == REJECTED) {
            w.close();
            return REJECTED;
        }
        return w;
    }

    default AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        Worker w = worker();
        AutoDisposable d = w.schedulePeriodically(() -> {
            try {
                task.run();
            } catch (Throwable ex) {
                w.close();
                FolyamPlugins.onError(ex);
            }
        }, initialDelay, period, unit);
        if (d == REJECTED) {
            w.close();
            return REJECTED;
        }
        return w;
    }

    Worker worker();

    default long now(TimeUnit unit) {
        return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    default void start() {
        // default no op
    }

    default void shutdown() {
        // default no op
    }

    interface Worker extends AutoDisposable {

        default AutoDisposable schedule(Runnable task) {
            return schedule(task, 0L, TimeUnit.NANOSECONDS);
        }

        AutoDisposable schedule(Runnable task, long delay, TimeUnit unit);

        default AutoDisposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            Objects.requireNonNull(task, "task == null");
            PeriodicTask t = new PeriodicTask(this, task, period,  unit,now(unit) + initialDelay);
            AutoDisposable d = schedule(t, initialDelay, unit);
            if (d != REJECTED) {
                t.setFirst(d);
                return t;
            }
            return REJECTED;
        }

        default long now(TimeUnit unit) {
            return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }

    AutoDisposable REJECTED = new Rejected();
}
