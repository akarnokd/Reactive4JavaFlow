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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class FolyamConcatMapEagerTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).concatMapEager(Folyam::just),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .concatMapEager(Folyam::just),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHidden2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .concatMapEager(v -> Folyam.just(v).hide()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapEager(v -> Folyam.range(v, 2)),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapEager(v -> Folyam.range(v, 2)
                                .subscribeOn(SchedulerServices.computation()), 2),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6
        );
    }


    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .concatMapEagerDelayError(v -> Folyam.range(v, 2)
                                .subscribeOn(SchedulerServices.computation()), 2),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.concatMapEager(v -> Folyam.range(v, 2)),
                IOException.class);
    }


    @Test
    public void errorDelayed() {
        TestHelper.assertFailureComposed(-1,
                f -> f.concatMapEagerDelayError(v -> Folyam.range(v, 2)),
                IOException.class);
    }

    @Test
    public void error2() {
        Folyam.range(1, 3)
                .concatMapEager(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                }, Integer.MAX_VALUE, 1)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void error2Delayed() {
        Folyam.range(1, 3)
                .concatMapEagerDelayError(v -> {
                    if (v == 2) {
                        return Folyam.error(new IOException());
                    }
                    return Folyam.just(v);
                }, Integer.MAX_VALUE, 1)
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void error3Delayed() {
        Folyam.range(1, 3).concatWith(Folyam.error(new IOException()))
                .concatMapEagerDelayError(v -> Folyam.just(v).subscribeOn(SchedulerServices.computation()), Integer.MAX_VALUE, 1)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertFailure(IOException.class, 1, 2, 3);
    }

    @Test
    public void normal() {
        Folyam.range(1, 5).concatMapEager(Folyam::just)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void longStream() {
        Folyam.range(0, 2)
                .concatMapEager(v ->
                        Folyam.range(v * 1000, 1000).subscribeOn(SchedulerServices.computation())
                )
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertValueCount(2000)
                .forEach((idx, v) -> assertEquals(idx.intValue(), v.intValue()))
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longStreamConditional() {
        Folyam.range(0, 2)
                .concatMapEager(v ->
                        Folyam.range(v * 1000, 1000).subscribeOn(SchedulerServices.computation())
                )
                .filter(v -> true)
                .test()
                .awaitDone(5, TimeUnit.SECONDS)
                .assertValueCount(2000)
                .forEach((idx, v) -> assertEquals(idx.intValue(), v.intValue()))
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void array() {
        Folyam.concatArrayEager(Folyam.just(1), Folyam.just(2))
                .test()
                .assertResult(1, 2);
    }


    @Test
    public void arrayDelayed() {
        Folyam.concatArrayEagerDelayError(Folyam.just(1), Folyam.error(new IOException()), Folyam.just(3))
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void iterable() {
        Folyam.concatEager(List.of(Folyam.just(1), Folyam.just(2)))
                .test()
                .assertResult(1, 2);
    }


    @Test
    public void iterablePrefetch() {
        Folyam.concatEager(List.of(Folyam.just(1), Folyam.just(2)), 1)
                .test()
                .assertResult(1, 2);
    }

    @Test
    public void iterableDelayed() {
        Folyam.concatEagerDelayError(List.of(Folyam.just(1), Folyam.error(new IOException()), Folyam.just(3)))
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void iterableDelayedPrefetch() {
        Folyam.concatEagerDelayError(List.of(Folyam.just(1), Folyam.error(new IOException()), Folyam.just(3)), 1)
                .test()
                .assertFailure(IOException.class, 1, 3);
    }

    @Test
    public void mapperCrash() {
        Folyam.range(1, 5)
                .concatMapEager(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedSourceThrows() {
        TestHelper.assertFailureComposed(2,
                f -> f.concatMapEager(v -> s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC))),
                IOException.class
        );
    }


    @Test
    public void fusedSourceThrowsDelayed() {
        TestHelper.assertFailureComposed(1,
                f -> f.concatMapEagerDelayError(v -> s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC))),
                IOException.class
        );
    }

    @Test
    public void errorBackpressured() {
        Folyam.error(new IOException()).hide()
                .concatMapEager(Folyam::just)
                .test(0)
                .assertFailure(IOException.class);
    }


    @Test
    public void errorBackpressuredConditional() {
        Folyam.error(new IOException()).hide()
                .concatMapEager(Folyam::just)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressured() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        TestConsumer<Integer> tc = Folyam.just(1).hide()
                .concatMapEager(v -> dp)
                .test(0);

        dp.onError(new IOException());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressuredConditional() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        TestConsumer<Integer> tc = Folyam.just(1).hide()
                .concatMapEager(v -> dp)
                .filter(v -> true)
                .test(0);

        dp.onError(new IOException());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressured1() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        TestConsumer<Integer> tc = Folyam.just(1).hide()
                .concatMapEager(v -> dp)
                .test(1);

        dp.onError(new IOException());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void errorInnerBackpressured1Conditional() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();
        TestConsumer<Integer> tc = Folyam.just(1).hide()
                .concatMapEager(v -> dp)
                .filter(v -> true)
                .test(1);

        dp.onError(new IOException());

        tc.assertFailure(IOException.class);
    }

    @Test
    public void iterableEmpty() {
        Folyam.concatEager(List.of())
                .test()
                .assertResult();
    }

    @Test
    public void iterableCrash() {
        Folyam.concatEager(new FailingMappedIterable<>(1, 10, 10, v -> Folyam.never()))
                .test()
                .assertFailure(IllegalStateException.class);
    }
}
