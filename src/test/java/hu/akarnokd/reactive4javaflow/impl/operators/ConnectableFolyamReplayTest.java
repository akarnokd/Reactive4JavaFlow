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
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ConnectableFolyamReplayTest {

    @Test
    public void unbounded() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replay();

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test(0);

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        tc2.assertEmpty();

        tc2.requestMore(20);

        tc2.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.test().assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.reset();

        tc1 = cf.test();

        tc1.assertEmpty();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    }


    @Test
    public void sizeBound() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(10);

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test(0);

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        tc2.assertEmpty();

        tc2.requestMore(20);

        tc2.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.test().assertResult(
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.reset();

        tc1 = cf.test();

        tc1.assertEmpty();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    }

    @Test
    public void timebound() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(1, TimeUnit.DAYS, SchedulerServices.single());

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test(0);

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        tc2.assertEmpty();

        tc2.requestMore(20);

        tc2.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.test().assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.reset();

        tc1 = cf.test();

        tc1.assertEmpty();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    }

    @Test
    public void timeAndSizeBound() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(10, 1, TimeUnit.DAYS, SchedulerServices.single());

        TestConsumer<Integer> tc1 = cf.test();
        TestConsumer<Integer> tc2 = cf.test(0);

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        tc2.assertEmpty();

        tc2.requestMore(20);

        tc2.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.test().assertResult(
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

        cf.reset();

        tc1 = cf.test();

        tc1.assertEmpty();

        cf.connect();

        tc1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    }

    @Test
    public void unboundedConnectFirst() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replay();
        cf.connect();
        cf.connect();

        cf.test().assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

    }

    @Test
    public void sizeBoundConnectFirst() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(10);
        cf.connect();
        cf.connect();

        cf.test().assertResult(
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

    }

    @Test
    public void timeBoundConnectFirst() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(1, TimeUnit.DAYS, SchedulerServices.single());
        cf.connect();
        cf.connect();

        cf.test().assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

    }

    @Test
    public void timeAndSizeBoundConnectFirst() {
        ConnectableFolyam<Integer> cf = Folyam.range(1, 20).replayLast(10, 1, TimeUnit.DAYS, SchedulerServices.single());
        cf.connect();
        cf.connect();

        cf.test().assertResult(
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20);

    }
}
