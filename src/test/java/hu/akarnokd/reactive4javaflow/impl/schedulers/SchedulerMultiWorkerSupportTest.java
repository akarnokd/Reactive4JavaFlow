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

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.disposables.CompositeAutoDisposable;
import org.junit.Test;

public class SchedulerMultiWorkerSupportTest {

    final int max = ((ParallelSchedulerService) SchedulerServices.computation()).parallelism;

    @Test
    public void moreThanMaxWorkers() {
        final List<SchedulerService.Worker> list = new ArrayList<>();

        SchedulerMultiWorkerSupport mws = (SchedulerMultiWorkerSupport)SchedulerServices.computation();

        mws.createWorkers(max * 2, (i, w) -> list.add(w));

        assertEquals(max * 2, list.size());
    }

    @Test
    public void getShutdownWorkers() {
        final List<SchedulerService.Worker> list = new ArrayList<>();

        SchedulerService parallel = SchedulerServices.newParallel(2, "A");
        parallel.shutdown();

        ((SchedulerMultiWorkerSupport)parallel).createWorkers(max * 2, (i, w) -> list.add(w));

        assertEquals(max * 2, list.size());

        for (SchedulerService.Worker w : list) {
            assertEquals(ParallelSchedulerService.STOPPED, ((ScheduledExecutorServiceWorker)w).exec);
        }
    }

    @Test
    public void distinctThreads() throws Exception {
        for (int i = 0; i < 1000; i++) {

            final CompositeAutoDisposable composite = new CompositeAutoDisposable();

            try {
                final CountDownLatch cdl = new CountDownLatch(max * 2);

                final Set<String> threads1 = Collections.synchronizedSet(new HashSet<String>());

                final Set<String> threads2 = Collections.synchronizedSet(new HashSet<String>());

                Runnable parallel1 = () -> {
                    final List<SchedulerService.Worker> list1 = new ArrayList<>();

                    SchedulerMultiWorkerSupport mws = (SchedulerMultiWorkerSupport)SchedulerServices.computation();

                    mws.createWorkers(max, (i1, w) -> {
                        list1.add(w);
                        composite.add(w);
                    });

                    Runnable run = () -> {
                        threads1.add(Thread.currentThread().getName());
                        cdl.countDown();
                    };

                    for (SchedulerService.Worker w : list1) {
                        w.schedule(run);
                    }
                };

                Runnable parallel2 = () -> {
                    final List<SchedulerService.Worker> list2 = new ArrayList<>();

                    SchedulerMultiWorkerSupport mws = (SchedulerMultiWorkerSupport)SchedulerServices.computation();

                    mws.createWorkers(max, (i12, w) -> {
                        list2.add(w);
                        composite.add(w);
                    });

                    Runnable run = () -> {
                        threads2.add(Thread.currentThread().getName());
                        cdl.countDown();
                    };

                    for (SchedulerService.Worker w : list2) {
                        w.schedule(run);
                    }
                };

                TestHelper.race(parallel1, parallel2);

                assertTrue(cdl.await(5, TimeUnit.SECONDS));

                assertEquals(threads1.toString(), max, threads1.size());
                assertEquals(threads2.toString(), max, threads2.size());
            } finally {
                composite.close();
            }
        }
    }
}
