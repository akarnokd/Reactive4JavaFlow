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

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;

public final class PeriodicTask implements Runnable, AutoDisposable {

    final SchedulerService.Worker worker;

    final Runnable actual;

    final long period;

    final TimeUnit unit;

    final long start;

    long count;

    AutoDisposable first;
    static final VarHandle FIRST = VH.find(MethodHandles.lookup(), PeriodicTask.class, "first", AutoDisposable.class);

    AutoDisposable next;
    static final VarHandle NEXT = VH.find(MethodHandles.lookup(), PeriodicTask.class, "next", AutoDisposable.class);

    public PeriodicTask(SchedulerService.Worker worker, Runnable actual, long period, TimeUnit unit, long start) {
        this.worker = worker;
        this.actual = actual;
        this.period = period;
        this.unit = unit;
        this.start = start;
    }


    @Override
    public void run() {
        try {
            actual.run();
        } catch (Throwable ex) {
            FolyamPlugins.onError(ex);
            return;
        }

        SchedulerService.Worker w = this.worker;

        long time = start + (++count) * period;
        long now = w.now(unit);
        long delay = Math.max(0L, time - now);

        AutoDisposable d = w.schedule(this, delay, unit);
        DisposableHelper.replace(this, NEXT, d);
    }

    @Override
    public void close() {
        DisposableHelper.close(this, FIRST);
        DisposableHelper.close(this, NEXT);
    }

    public void setFirst(AutoDisposable d) {
        DisposableHelper.replace(this, FIRST, d);
    }
}
