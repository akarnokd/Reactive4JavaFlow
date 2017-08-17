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
import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.*;
import hu.akarnokd.reactive4javaflow.impl.schedulers.TrampolineSchedulerService;

import java.lang.invoke.*;
import java.util.concurrent.TimeUnit;

public final class FolyamTimer extends Folyam<Long> {

    final long delay;

    final TimeUnit unit;

    final SchedulerService executor;

    public FolyamTimer(long delay, TimeUnit unit, SchedulerService executor) {
        this.delay = delay;
        this.unit = unit;
        this.executor = executor;
    }

    @Override
    protected void subscribeActual(FolyamSubscriber<? super Long> s) {
        TimerSubscription parent = new TimerSubscription(s);
        s.onSubscribe(parent);
        SchedulerService exec = executor;
        if (exec instanceof TrampolineSchedulerService) {
            SchedulerService.Worker w = exec.worker();
            parent.setTask(w);
            w.schedule(parent, delay, unit);
        } else {
            parent.setTask(exec.schedule(parent, delay, unit));
        }
    }

    static final class TimerSubscription extends DeferredScalarSubscription<Long> implements Runnable {

        AutoDisposable task;
        static final VarHandle TASK = VH.find(MethodHandles.lookup(), TimerSubscription.class, "task", AutoDisposable.class);

        public TimerSubscription(FolyamSubscriber<? super Long> actual) {
            super(actual);
        }

        @Override
        public void run() {
            complete(0L);
        }

        void setTask(AutoDisposable d) {
            DisposableHelper.replace(this, TASK, d);
        }

        @Override
        public void cancel() {
            super.cancel();
            DisposableHelper.close(this, TASK);
        }
    }
}
