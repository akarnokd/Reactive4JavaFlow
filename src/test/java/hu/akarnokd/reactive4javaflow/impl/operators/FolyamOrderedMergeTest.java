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
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class FolyamOrderedMergeTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.orderedMergeArray(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.orderedMergeArray(Folyam.fromArray(1, 3, 5, 7, 9).hide(), Folyam.fromArray(2, 4, 6, 8, 10).hide())
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.orderedMergeArray(1, Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.orderedMerge(List.of(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10)))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.orderedMerge(List.of(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10)), 1)
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }


    @Test
    public void standardDelayError() {
        TestHelper.assertResult(
                Folyam.orderedMergeArrayDelayError(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }


    @Test
    public void standardHiddenDelayError() {
        TestHelper.assertResult(
                Folyam.orderedMergeArrayDelayError(Folyam.fromArray(1, 3, 5, 7, 9).hide(), Folyam.fromArray(2, 4, 6, 8, 10).hide())
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard1DelayError() {
        TestHelper.assertResult(
                Folyam.orderedMergeArrayDelayError(1, Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard2DelayError() {
        TestHelper.assertResult(
                Folyam.orderedMergeDelayError(List.of(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10)))
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard3DelayError() {
        TestHelper.assertResult(
                Folyam.orderedMergeDelayError(List.of(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.fromArray(2, 4, 6, 8, 10)), 1)
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void manySourcesAndCrash() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.never());
        }
        list.add(null);
        Folyam.orderedMerge(list)
        .test()
        .assertFailure(NullPointerException.class);
    }

    @Test
    public void firstError() {
        Folyam.orderedMergeArray(Folyam.error(new IOException()), Folyam.fromArray(2, 4, 6, 8, 10))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void firstErrorBackpressured() {
        Folyam.orderedMergeArray(Folyam.error(new IOException()), Folyam.fromArray(2, 4, 6, 8, 10))
                .test(0L)
                .assertFailure(IOException.class);
    }

    @Test
    public void secondError() {
        Folyam.orderedMergeArray(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void delayError() {
        Folyam.orderedMergeArrayDelayError(Folyam.fromArray(1, 3, 5, 7, 9), Folyam.error(new IOException()), Folyam.fromArray(2, 4, 6, 8, 10))
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void emptySources() {
        Folyam.orderedMergeArray()
                .test()
                .assertResult();
    }


    @Test
    public void oneSource() {
        TestHelper.assertResult(Folyam.orderedMergeArray(Folyam.range(1, 5)), 1, 2, 3, 4, 5);
    }

    @Test
    public void nullSource() {
        Folyam.orderedMergeArray(Folyam.range(1, 5), null)
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void fusedSourceCrash() {
        Folyam.orderedMergeArray(Folyam.range(1, 5), s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC)))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedSourceCrashBackpressured() {
        Folyam.orderedMergeArray(s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC)), Folyam.never())
                .test(0L)
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedSourceCrashBackpressuredDelayError() {
        Folyam.orderedMergeArrayDelayError(s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC)), Folyam.empty())
                .test(0L)
                .assertFailure(IOException.class);
    }

    @Test
    public void fusedSourceCrashDelayError() {
        Folyam.orderedMergeArrayDelayError(Folyam.range(1, 5), s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC)))
                .test()
                .assertFailure(IOException.class, 1, 2, 3, 4, 5);
    }

    @Test
    public void comparatorCrash() {
        Folyam.orderedMergeArray((a, b) -> { throw new NullPointerException(); },
                Folyam.range(1, 5), Folyam.range(11, 5))
            .test()
            .assertFailure(NullPointerException.class);
    }


    @Test
    public void comparatorCrashDelayError() {
        Folyam.orderedMergeArrayDelayError((a, b) -> { throw new NullPointerException(); },
                Folyam.range(1, 5), Folyam.range(11, 5))
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void comparatorCrashIterator() {
        Folyam.orderedMerge(List.of(Folyam.range(1, 5), Folyam.range(11, 5)), (a, b) -> { throw new NullPointerException(); })
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void comparatorCrashIteratorDelayError() {
        Folyam.orderedMergeDelayError(List.of(Folyam.range(1, 5), Folyam.range(11, 5)), (a, b) -> { throw new NullPointerException(); })
                .test()
                .assertFailure(NullPointerException.class);
    }

    @Test
    public void takeBackpressured() {
        Folyam.orderedMergeArray(Folyam.just(1), Folyam.just(2))
                .take(1)
                .test(1)
                .assertResult(1);
    }
}
