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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import hu.akarnokd.reactive4javaflow.*;
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.functionals.*;
import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;
import org.junit.*;

public class ParallelFolyamTest {

    @Test
    public void sequentialMode() {
        Folyam<Integer> source = Folyam.range(1, 1000000).hide();
        for (int i = 1; i < 33; i++) {
            Folyam<Integer> result = ParallelFolyam.fromPublisher(source, i)
            .map(v -> v + 1)
            .sequential()
            ;

            TestConsumer<Integer> ts = new TestConsumer<>();

            result.subscribe(ts);

            ts
            .assertValueCount(1000000)
            .assertComplete()
            .assertNoErrors()
            ;
        }

    }

    @Test
    public void sequentialModeFused() {
        Folyam<Integer> source = Folyam.range(1, 1000000);
        for (int i = 1; i < 33; i++) {
            Folyam<Integer> result = ParallelFolyam.fromPublisher(source, i)
            .map(v -> v + 1)
            .sequential()
            ;

            TestConsumer<Integer> ts = new TestConsumer<>();

            result.subscribe(ts);

            ts
            .assertValueCount(1000000)
            .assertComplete()
            .assertNoErrors()
            ;
        }

    }

    @Test
    public void parallelMode() {
        Folyam<Integer> source = Folyam.range(1, 1000000).hide();
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {

            ExecutorService exec = Executors.newFixedThreadPool(i);

            SchedulerService scheduler = SchedulerServices.newExecutor(exec);

            try {
                Folyam<Integer> result = ParallelFolyam.fromPublisher(source, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .sequential()
                ;

                TestConsumer<Integer> ts = new TestConsumer<>();

                result.subscribe(ts);

                ts.awaitDone(10, TimeUnit.SECONDS);

                ts
                .assertValueCount(1000000)
                .assertComplete()
                .assertNoErrors()
                ;
            } finally {
                exec.shutdown();
            }
        }

    }

    @Test
    public void parallelModeFused() {
        Folyam<Integer> source = Folyam.range(1, 1000000);
        int ncpu = Math.max(8, Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < ncpu + 1; i++) {

            ExecutorService exec = Executors.newFixedThreadPool(i);

            SchedulerService scheduler = SchedulerServices.newExecutor(exec);

            try {
                Folyam<Integer> result = ParallelFolyam.fromPublisher(source, i)
                .runOn(scheduler)
                .map(v -> v + 1)
                .sequential()
                ;

                TestConsumer<Integer> ts = new TestConsumer<>();

                result.subscribe(ts);

                ts.awaitDone(10, TimeUnit.SECONDS);

                ts
                .assertValueCount(1000000)
                .assertComplete()
                .assertNoErrors()
                ;
            } finally {
                exec.shutdown();
            }
        }

    }

    @Test
    public void reduceFull() {
        for (int i = 1; i <= Runtime.getRuntime().availableProcessors() * 2; i++) {
            TestConsumer<Integer> ts = new TestConsumer<>();

            Folyam.range(1, 10)
            .parallel(i)
            .reduce((a, b) -> a + b)
            .subscribe(ts);

            ts.assertResult(55);
        }
    }

    @Test
    public void parallelReduceFull() {
        int m = 100000;
        for (int n = 1; n <= m; n *= 10) {
//            System.out.println(n);
            for (int i = 1; i <= Runtime.getRuntime().availableProcessors(); i++) {
//                System.out.println("  " + i);

                ExecutorService exec = Executors.newFixedThreadPool(i);

                SchedulerService scheduler = SchedulerServices.newExecutor(exec);

                try {
                    TestConsumer<Long> ts = new TestConsumer<>();

                    Folyam.range(1, n)
                    .map(v -> (long)v)
                    .parallel(i)
                    .runOn(scheduler)
                    .reduce((a, b) -> a + b)
                    .subscribe(ts);

                    ts.awaitDone(500, TimeUnit.SECONDS);

                    long e = ((long)n) * (1 + n) / 2;

                    ts.assertResult(e);
                } finally {
                    exec.shutdown();
                }
            }
        }
    }

    /*
    @SuppressWarnings("unchecked")
    @Test
    public void toSortedList() {
        TestConsumer<List<Integer>> ts = new TestConsumer<List<Integer>>();

        Folyam.fromArray(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
        .parallel()
        .toSortedList(Functions.naturalComparator())
        .subscribe(ts);

        ts.assertResult(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }
    */

    @Test
    public void sorted() {
        TestConsumer<Integer> ts = new TestConsumer<>(0);

        Folyam.fromArray(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
        .parallel()
        .sorted(Comparator.naturalOrder())
        .subscribe(ts);

        ts.assertEmpty();

        ts.requestMore(2);

        ts.assertValues(1, 2);

        ts.requestMore(5);

        ts.assertValues(1, 2, 3, 4, 5, 6, 7);

        ts.requestMore(3);

        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void collect() {
        Callable<List<Integer>> as = ArrayList::new;

        TestConsumer<Integer> ts = new TestConsumer<>();
        Folyam.range(1, 10)
        .parallel()
        .collect(as, List::add)
        .sequential()
        .flatMapIterable((CheckedFunction<List<Integer>, Iterable<Integer>>) v -> v)
        .subscribe(ts);

        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoErrors()
        .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void from() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        ParallelFolyam.fromArray(Folyam.range(1, 5), Folyam.range(6, 5))
        .sequential()
        .subscribe(ts);

        ts.assertValueSet(new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void concatMapUnordered() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.range(1, 5)
        .parallel()
        .concatMap((CheckedFunction<Integer, Flow.Publisher<Integer>>) v -> Folyam.range(v * 10 + 1, 3))
        .sequential()
        .subscribe(ts);

        ts.assertValueSet(new HashSet<>(Arrays.asList(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)))
        .assertNoErrors()
        .assertComplete();

    }

    @Test
    public void flatMapUnordered() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.range(1, 5)
        .parallel()
        .flatMap((CheckedFunction<Integer, Flow.Publisher<Integer>>) v -> Folyam.range(v * 10 + 1, 3))
        .sequential()
        .subscribe(ts);

        ts.assertValueSet(new HashSet<>(Arrays.asList(11, 12, 13, 21, 22, 23, 31, 32, 33, 41, 42, 43, 51, 52, 53)))
        .assertNoErrors()
        .assertComplete();

    }

    @Test
    public void collectAsyncFused() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(100000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }

    @Test
    public void collectAsync() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000).hide()
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(100000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }


    @Test
    public void collectAsync2() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000).hide()
            .observeOn(s)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(100000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }

    @Test
    public void collectAsync3() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000).hide()
            .observeOn(s)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(100000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }


    @Test
    public void collectAsync3Fused() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000)
            .observeOn(s)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(100000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }

    @Test
    public void collectAsync3Take() {
        ExecutorService exec = Executors.newFixedThreadPool(4);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            Folyam.range(1, 100000)
            .take(1000)
            .observeOn(s)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(5, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(1000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }

    @Test
    public void collectAsync4Take() {
        ExecutorService exec = Executors.newFixedThreadPool(3);

        SchedulerService s = SchedulerServices.newExecutor(exec);

        try {
            Callable<List<Integer>> as = ArrayList::new;
            TestConsumer<List<Integer>> ts = new TestConsumer<>();

            SolocastProcessor<Integer> up = new SolocastProcessor<>();

            for (int i = 0; i < 1000; i++) {
                up.onNext(i);
            }

            up
            .take(1000)
            .observeOn(s)
            .parallel(3)
            .runOn(s)
            .collect(as, List::add)
            .doOnNext(v -> System.out.println(v.size()))
            .sequential()
            .subscribe(ts);

            ts.awaitDone(500, TimeUnit.SECONDS);
            ts.assertValueCount(3)
            .assertNoErrors()
            .assertComplete()
            ;

            List<List<Integer>> list = ts.values();

            Assert.assertEquals(1000, list.get(0).size() + list.get(1).size() + list.get(2).size());
        } finally {
            exec.shutdown();
        }
    }

    @Test
    public void emptySourceZeroRequest() {
        TestConsumer<Object> ts = new TestConsumer<>(0);

        Folyam.range(1, 3).parallel(3).sequential().subscribe(ts);

        ts.requestMore(1);

        ts.assertValues(1);
    }

    @Test
    public void parallelismAndPrefetch1() {
        int parallelism = 1;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .map(v -> v)
                    .sequential()
                    .test()
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetch2() {
        int parallelism = 2;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .map(v -> v)
                    .sequential()
                    .test()
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetch4() {
        int parallelism = 4;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .map(v -> v)
                    .sequential()
                    .test()
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetch8() {
        int parallelism = 8;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .map(v -> v)
                    .sequential()
                    .test()
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetchAsync1() {
        int parallelism = 1;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            //System.out.println("parallelismAndPrefetchAsync >> " + parallelism + ", " + prefetch);

            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .runOn(SchedulerServices.computation())
                    .map(v -> v)
                    .sequential(prefetch)
                    .test()
                    .withTag("parallelism = " + parallelism + ", prefetch = " + prefetch)
                    .awaitDone(30, TimeUnit.SECONDS)
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }


    @Test
    public void parallelismAndPrefetchAsync2() {
        int parallelism = 2;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            //System.out.println("parallelismAndPrefetchAsync >> " + parallelism + ", " + prefetch);

            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .runOn(SchedulerServices.computation())
                    .map(v -> v)
                    .sequential(prefetch)
                    .test()
                    .withTag("parallelism = " + parallelism + ", prefetch = " + prefetch)
                    .awaitDone(30, TimeUnit.SECONDS)
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetchAsync4() {
        int parallelism = 4;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            //System.out.println("parallelismAndPrefetchAsync >> " + parallelism + ", " + prefetch);

            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .runOn(SchedulerServices.computation())
                    .map(v -> v)
                    .sequential(prefetch)
                    .test()
                    .withTag("parallelism = " + parallelism + ", prefetch = " + prefetch)
                    .awaitDone(30, TimeUnit.SECONDS)
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @Test
    public void parallelismAndPrefetchAsync8() {
        int parallelism = 8;
        for (int prefetch = 1; prefetch <= 1024; prefetch *= 2) {
            //System.out.println("parallelismAndPrefetchAsync >> " + parallelism + ", " + prefetch);

            Folyam.range(1, 1024 * 1024)
                    .parallel(parallelism, prefetch)
                    .runOn(SchedulerServices.computation())
                    .map(v -> v)
                    .sequential(prefetch)
                    .test()
                    .withTag("parallelism = " + parallelism + ", prefetch = " + prefetch)
                    .awaitDone(30, TimeUnit.SECONDS)
                    .assertValueCount(1024 * 1024)
                    .assertNoErrors()
                    .assertComplete();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void badParallelismStage() {
        TestConsumer<Integer> ts = new TestConsumer<>();

        Folyam.range(1, 10)
        .parallel(2)
        .subscribe(new FolyamSubscriber[] { ts });

        ts.assertFailure(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void badParallelismStage2() {
        TestConsumer<Integer> ts1 = new TestConsumer<>();
        TestConsumer<Integer> ts2 = new TestConsumer<>();
        TestConsumer<Integer> ts3 = new TestConsumer<>();

        Folyam.range(1, 10)
        .parallel(2)
        .subscribe(new FolyamSubscriber[] { ts1, ts2, ts3 });

        ts1.assertFailure(IllegalArgumentException.class);
        ts2.assertFailure(IllegalArgumentException.class);
        ts3.assertFailure(IllegalArgumentException.class);
    }

    @Test
    public void filter() {
        Folyam.range(1, 20)
        .parallel()
        .runOn(SchedulerServices.computation())
        .filter(v -> v % 2 == 0)
        .sequential()
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertValueSet(Arrays.asList(2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void filterThrows() throws Exception {
        final boolean[] cancelled = { false };
        Folyam.range(1, 20).concatWith(Folyam.<Integer>never())
        .doOnCancel(() -> cancelled[0] = true)
        .parallel()
        .runOn(SchedulerServices.computation())
        .filter(v -> {
            if (v == 10) {
                throw new IOException();
            }
            return v % 2 == 0;
        })
        .sequential()
        .test()
        .awaitDone(5, TimeUnit.SECONDS)
        .assertError(IOException.class)
        .assertNotComplete();

        Thread.sleep(100);

        assertTrue(cancelled[0]);
    }

    @Test
    public void doAfterNext() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel()
        .doAfterNext(v -> count[0]++)
        .sequential()
        .test()
        .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void doOnNextThrows() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel()
        .doOnNext(v -> {
            if (v == 3) {
                throw new IOException();
            } else {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertError(IOException.class)
        .assertNotComplete();

        assertTrue("" + count[0], count[0] < 5);
    }

    @Test
    public void doAfterNextThrows() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel()
        .doAfterNext(v -> {
            if (v == 3) {
                throw new IOException();
            } else {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertError(IOException.class)
        .assertNotComplete();

        assertTrue("" + count[0], count[0] < 5);
    }

    @Test
    public void errorNotRepeating() throws Exception {
        TestHelper.withErrorTracking(errors -> {
            Folyam.error(new IOException())
                    .parallel()
                    .runOn(SchedulerServices.computation())
                    .sequential()
                    .test()
                    .awaitDone(5, TimeUnit.SECONDS)
                    .assertFailure(IOException.class)
            ;

            Thread.sleep(300);

            for (Throwable ex : errors) {
                ex.printStackTrace();
            }
            assertTrue(errors.toString(), errors.isEmpty());
        });
    }

    @Test
    public void doOnError() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .map(v -> {
            if (v == 3) {
                throw new IOException();
            }
            return v;
        })
        .doOnError(e -> {
            if (e instanceof IOException) {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertError(IOException.class)
        .assertNotComplete();

        assertEquals(1, count[0]);
    }

    @Test
    public void doOnErrorThrows() {
        TestConsumer<Integer> ts = Folyam.range(1, 5)
        .parallel(2)
        .map(v -> {
            if (v == 3) {
                throw new IOException();
            }
            return v;
        })
        .doOnError(e -> {
            if (e instanceof IOException) {
                throw new IOException();
            }
        })
        .sequential()
        .test()
        .assertError(CompositeThrowable.class)
        .assertNotComplete()
        .assertInnerErrors(errors -> {
            TestHelper.assertError(errors, 0, IOException.class);
            TestHelper.assertError(errors, 1, IOException.class);
        });
    }

    @Test
    public void doOnComplete() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .doOnComplete(() -> count[0]++)
        .sequential()
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(2, count[0]);
    }

    @Test
    public void doOnSubscribe() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .doOnSubscribe(s -> count[0]++)
        .sequential()
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(2, count[0]);
    }

    @Test
    public void doOnRequest() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .doOnRequest(s -> count[0]++)
        .sequential()
        .test()
        .assertResult(1, 2, 3, 4, 5);

        assertEquals(2, count[0]);
    }

    @Test
    public void doOnCancel() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .doOnCancel(() -> count[0]++)
        .sequential()
        .take(2)
        .test()
        .assertResult(1, 2);

        assertEquals(2, count[0]);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void fromPublishers() {
        ParallelFolyam.fromArray(new Flow.Publisher[0]);
    }

    @Test
    public void to() {
        Folyam.range(1, 5)
        .parallel()
        .to(ParallelFolyam::sequential)
        .test()
        .assertResult(1, 2, 3, 4, 5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void toThrows() {
        Folyam.range(1, 5)
        .parallel()
        .to((Function<ParallelFolyam<Integer>, Folyam<Integer>>) pf -> {
            throw new IllegalArgumentException();
        });
    }

    @Test
    public void compose() {
        Folyam.range(1, 5)
        .parallel()
        .compose(pf -> pf.map(v -> v + 1))
        .sequential()
        .test()
        .assertResult(2, 3, 4, 5, 6);
    }

    @Test
    public void flatMapDelayError() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .flatMapDelayError((CheckedFunction<Integer, Folyam<Integer>>) v -> {
            if (v == 3) {
               return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        })
        .doOnError(e -> {
            if (e instanceof IOException) {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertValues(1, 2, 4, 5)
        .assertError(IOException.class)
        .assertNotComplete();

        assertEquals(1, count[0]);
    }

    @Test
    public void flatMapDelayErrorMaxConcurrency() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .flatMapDelayError((CheckedFunction<Integer, Folyam<Integer>>) v -> {
            if (v == 3) {
               return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        }, 1)
        .doOnError(e -> {
            if (e instanceof IOException) {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertValues(1, 2, 4, 5)
        .assertError(IOException.class)
        .assertNotComplete();

        assertEquals(1, count[0]);
    }

    @Test
    public void concatMapDelayError() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .concatMapDelayError((CheckedFunction<Integer, Folyam<Integer>>) v -> {
            if (v == 3) {
               return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        })
        .doOnError(e -> {
            if (e instanceof IOException) {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertValues(1, 2, 4, 5)
        .assertError(IOException.class)
        .assertNotComplete();

        assertEquals(1, count[0]);
    }

    @Test
    public void concatMapDelayErrorPrefetch() {
        final int[] count = { 0 };

        Folyam.range(1, 5)
        .parallel(2)
        .concatMapDelayError((CheckedFunction<Integer, Folyam<Integer>>) v -> {
            if (v == 3) {
               return Folyam.error(new IOException());
            }
            return Folyam.just(v);
        }, 1)
        .doOnError(e -> {
            if (e instanceof IOException) {
                count[0]++;
            }
        })
        .sequential()
        .test()
        .assertValues(1, 2, 4, 5)
        .assertError(IOException.class)
        .assertNotComplete();

        assertEquals(1, count[0]);
    }

    public static void checkSubscriberCount(ParallelFolyam<?> source) {
        int n = source.parallelism();

        @SuppressWarnings("unchecked")
        TestConsumer<Object>[] consumers = new TestConsumer[n + 1];

        for (int i = 0; i <= n; i++) {
            consumers[i] = new TestConsumer<>();
        }

        source.subscribe(consumers);

        for (int i = 0; i <= n; i++) {
            consumers[i].awaitDone(5, TimeUnit.SECONDS)
            .assertFailure(IllegalArgumentException.class);
        }
    }

    @Test
    public void concatMapSubscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .concatMap(v -> Folyam.just(1)));
    }

    @Test
    public void flatMapSubscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(Folyam.range(1, 5).parallel()
        .flatMap(v -> Folyam.just(1)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void fromArraySubscriberCount() {
        ParallelFolyamTest.checkSubscriberCount(ParallelFolyam.fromArray(new Flow.Publisher[] { Folyam.just(1) }));
    }

    @Test
    public void flatMapMaxConcurrency() {
        Folyam.range(1, 10)
                .parallel()
                .flatMap(Esetleg::just, 1)
                .sequential()
                .test()
                .assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void fromPublisher() {
        SubmissionPublisher<Integer> sp = new SubmissionPublisher<>();
        TestConsumer<Integer> tc = ParallelFolyam
                .fromPublisher(sp)
                .sequential()
                .test();

        sp.submit(1);
        sp.submit(2);
        sp.submit(3);
        sp.submit(4);
        sp.submit(5);
        sp.close();

        tc.awaitDone(5, TimeUnit.SECONDS)
                .assertResult(1, 2, 3, 4, 5);
    }

}
