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
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class FolyamFlattenStreamTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).flatMapStream(v -> List.of(v).stream()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHide() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().flatMapStream(v -> List.of(v).stream()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHide1() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().flatMapStream(v -> List.of(v).stream(), 1),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.empty().flatMapStream(v -> List.of(v).stream())

        );
    }

    @Test
    public void standard2Hide() {
        TestHelper.assertResult(
                Folyam.empty().hide().flatMapStream(v -> List.of(v).stream())

        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).flatMapStream(v -> Stream.of(v, v + 1)),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6
        );
    }


    @Test
    public void standard3Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().flatMapStream(v -> Stream.of(v, v + 1)),
                1, 2, 2, 3, 3, 4, 4, 5, 5, 6
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.range(1, 5).flatMapStream(v -> Stream.empty())

        );
    }


    @Test
    public void standard4Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().flatMapStream(v -> Stream.empty())

        );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Folyam.range(1, 10).flatMapStream(v ->
                    v % 2 != 0 ? Stream.empty() : Stream.of(v)),
                2, 4, 6, 8, 10
        );
    }


    @Test
    public void standard5Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 10).hide().flatMapStream(v ->
                        v % 2 != 0 ? Stream.empty() : Stream.of(v)),
                2, 4, 6, 8, 10
        );
    }


    @Test
    public void standard6() {
        TestHelper.assertResult(
                Folyam.range(1, 10).flatMapStream(v ->
                        v % 2 == 0 ? Stream.empty() : Stream.of(v)),
                1, 3, 5, 7, 9
        );
    }


    @Test
    public void standard6Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 10).hide().flatMapStream(v ->
                        v % 2 == 0 ? Stream.empty() : Stream.of(v)),
                1, 3, 5, 7, 9
        );
    }

    @Test
    public void longSource1() {
        Folyam.range(1, 1000)
                .flatMapStream(v -> List.of(v).stream())
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longSource1Fused() {
        Folyam.range(1, 1000)
                .flatMapStream(v -> List.of(v).stream())
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longSource1Hidden() {
        Folyam.range(1, 1000)
                .hide()
                .flatMapStream(v -> List.of(v).stream())
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longSource1Conditional() {
        Folyam.range(1, 1000)
                .flatMapStream(v -> List.of(v).stream())
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longSource1FusedConditional() {
        Folyam.range(1, 1000)
                .flatMapStream(v -> List.of(v).stream())
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void longSource1HiddenConditional() {
        Folyam.range(1, 1000)
                .hide()
                .flatMapStream(v -> List.of(v).stream())
                .filter(v -> true)
                .test()
                .assertValueCount(1000)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                v -> v.flatMapStream(w -> List.of(w).stream()),
                IOException.class);
    }

    @Test
    public void errorSyncCrash() {
        TestHelper.assertFailureComposed(-2,
                v -> v.flatMapStream(w -> List.of(w).stream()),
                IOException.class);
    }


    @Test
    public void errorAsyncCrash() {
        TestHelper.assertFailureComposed(-3,
                v -> v.flatMapStream(w -> List.of(w).stream()),
                IOException.class);
    }

    @Test
    public void mapperCrash() {
        TestHelper.assertFailureComposed(5,
                v -> v.flatMapStream(w -> { throw new IOException(); }),
                IOException.class);
    }

    @Test
    public void iteratorCrash() {
        TestHelper.assertFailureComposed(5,
                v -> v.flatMapStream(w -> FolyamStreamTest.toStream(new FailingIterable(1, 10, 10))),
                IllegalStateException.class);
    }

    @Test
    public void hasNextCrash() {
        TestHelper.assertFailureComposed(5,
                v -> v.flatMapStream(w -> FolyamStreamTest.toStream(new FailingIterable(10, 1, 10))),
                IllegalStateException.class);
    }


    @Test
    public void hasNextCrash2() {
        TestHelper.assertFailureComposed(5,
                v -> v.flatMapStream(w -> FolyamStreamTest.toStream(new FailingIterable(10, 2, 10))),
                IllegalStateException.class, 1);
    }

    @Test
    public void onNextCrash() {
        TestHelper.assertFailureComposed(5,
                v -> v.flatMapStream(w -> FolyamStreamTest.toStream(new FailingIterable(10, 10, 1))),
                IllegalStateException.class);
    }

    @Test
    public void closed() {
        int[] counter = { 0 };
        Folyam.just(1)
                .flatMapStream(v -> List.of(1).stream().onClose(() -> counter[0]++))
                .test()
                .assertResult(1);

        assertEquals(1, counter[0]);
    }

    @Test
    public void closedConditional() {
        int[] counter = { 0 };
        Folyam.just(1)
                .flatMapStream(v -> List.of(1).stream().onClose(() -> counter[0]++))
                .filter(v -> true)
                .test()
                .assertResult(1);

        assertEquals(1, counter[0]);
    }
}
