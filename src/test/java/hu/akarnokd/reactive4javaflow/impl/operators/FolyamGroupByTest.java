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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FolyamGroupByTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).groupBy(v -> v).flatMap(v -> v),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void normal() {
        Folyam.range(1, 5)
                .groupBy(v -> v)
                .flatMap(v -> v)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalHide() {
        Folyam.range(1, 5)
                .groupBy(v -> v)
                .hide()
                .flatMap(Folyam::hide)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .groupBy(v -> v)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorWhenGroup() {
        Folyam.just(1).concatWith(Folyam.error(new IOException()))
                .groupBy(v -> v)
                .flatMapDelayError(v -> v)
                .test()
                .assertFailure(CompositeThrowable.class, 1)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IOException.class);
                });
    }


    @Test
    public void errorWhenGroupHide() {
        Folyam.just(1).concatWith(Folyam.error(new IOException()))
                .groupBy(v -> v)
                .flatMapDelayError(Folyam::hide)
                .test()
                .assertFailure(CompositeThrowable.class, 1)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class);
                    TestHelper.assertError(errors, 1, IOException.class);
                });
    }

    @Test
    public void oneGroup() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .groupBy(v -> 1)
                .take(1)
                .flatMap(v -> v), 1, 2, 3, 4, 5);
    }

    @Test
    public void oneGroupHide() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .groupBy(v -> 1)
                .take(1)
                .flatMap(Folyam::hide), 1, 2, 3, 4, 5);
    }

    @Test
    public void keySelectorCrash() {
        Folyam.just(1)
                .groupBy(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void keySelectorCrashBackpressured() {
        Folyam.just(1)
                .groupBy(v -> { throw new IOException(); })
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void valueSelectorCrash() {
        Folyam.just(1)
                .groupBy(v -> v, v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void valueSelectorCrashBackpressured() {
        Folyam.just(1)
                .groupBy(v -> v, v -> { throw new IOException(); })
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void cancelNoGroup() {
        Folyam.never()
                .groupBy(v -> v, v -> v, 1)
                .test()
                .cancel()
                .assertEmpty();
    }

    @Test
    public void mainFused() {
        Folyam.just(1)
                .groupBy(v -> v)
                .concatMap(v -> v)
                .test()
                .assertResult(1);
    }

    @Test
    public void rejectFusion() {
        Folyam.just(1)
                .groupBy(v -> v)
                .test(Long.MAX_VALUE, false, FusedSubscription.SYNC)
                .assertFusionMode(FusedSubscription.NONE)
                .assertValueCount(1)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void mainFusedClear() {
        Folyam.just(1)
                .groupBy(v -> v)
                .concatMap(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainFusedError() {
        Folyam.error(new IOException())
                .groupBy(v -> v)
                .concatMap(v -> v)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void empty() {
        Folyam.empty()
                .groupBy(v -> v)
                .test()
                .assertResult();
    }

    @Test
    public void emptyBackpressure() {
        Folyam.empty()
                .groupBy(v -> v)
                .test(0)
                .assertResult();
    }

    @Test
    public void groupCancel() {
        Folyam.just(1)
                .groupBy(v -> v)
                .doOnNext(v -> {
                    assertEquals(1, v.getKey().intValue());
                    v.test(0, true, FusedSubscription.NONE)
                    .assertEmpty();

                    v.test().assertFailure(IllegalStateException.class);
                })
                .test()
                .assertValueCount(1)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void innerSyncFusion() {
        TestConsumer<Integer> tc = new TestConsumer<>();
        tc.requestFusionMode(FusedSubscription.SYNC);

        Folyam.just(1)
                .groupBy(v -> v)
                .doOnNext(v -> {
                    v.subscribe(tc);
                })
                .test()
                .assertValueCount(1)
                .assertNoErrors()
                .assertComplete();

        tc
        .assertFusionMode(FusedSubscription.NONE)
        .assertResult(1);
    }

    @Test
    public void longGroup() {
        Folyam.range(1, 1000)
                .groupBy(v -> 1)
                .flatMap(v -> v)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longGroupHidden() {
        Folyam.range(1, 1000)
                .groupBy(v -> 1)
                .flatMap(Folyam::hide)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longGroup1Hidden() {
        Folyam.range(1, 1000)
                .groupBy(v -> 1, v -> v, 1)
                .flatMap(Folyam::hide)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void innerTake() {
        TestConsumer<Integer> tc = new TestConsumer<>(1);

        Folyam.just(1)
                .groupBy(v -> v)
                .doOnNext(v -> {
                    v.take(1).subscribe(tc);
                })
                .test()
                .assertValueCount(1)
                .assertNoErrors()
                .assertComplete();

        tc
        .assertResult(1);
    }

    @Test
    public void innerBackpressured() {
        TestConsumer<Integer> tc = new TestConsumer<>(1);

        Folyam.just(1)
                .groupBy(v -> v)
                .doOnNext(v -> {
                    v.subscribe(tc);
                })
                .test()
                .assertValueCount(1)
                .assertNoErrors()
                .assertComplete();

        tc
                .assertResult(1);
    }

    @Test
    public void innerBackpressuredError() {
        TestConsumer<Integer> tc = new TestConsumer<>(1);

        Folyam.just(1).concatWith(Folyam.error(new IOException()))
                .groupBy(v -> v)
                .doOnNext(v -> {
                    v.subscribe(tc);
                })
                .test()
                .assertValueCount(1)
                .assertError(IOException.class)
                .assertNotComplete();

        tc
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void groupClear() {
        Folyam.just(1)
                .groupBy(v -> v)
                .flatMap(v -> v.concatMap(w -> { throw new IOException(); }))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void flatMapInner() {
        Folyam.just(1).hide()
                .flatMap(v -> Folyam.just(1).groupBy(w -> w))
                .test(0)
                .assertEmpty();
    }
}
