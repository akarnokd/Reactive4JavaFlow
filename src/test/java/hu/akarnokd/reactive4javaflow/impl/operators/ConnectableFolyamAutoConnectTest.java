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
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import hu.akarnokd.reactive4javaflow.disposables.SequentialAutoDisposable;
import org.junit.Test;

import java.util.concurrent.CancellationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectableFolyamAutoConnectTest {

    @Test
    public void normal() {
        Folyam<Integer> f = Folyam.range(1, 5).publish().autoConnect(2);

        TestConsumer<Integer> tc1 = f.test();

        tc1.assertEmpty();

        TestConsumer<Integer> tc2 = f.test();

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult(1, 2, 3, 4, 5);
    }


    @Test
    public void normal0() {
        Folyam<Integer> f = Folyam.range(1, 5).publish().autoConnect(0);

        TestConsumer<Integer> tc1 = f.test();

        TestConsumer<Integer> tc2 = f.test();

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult();
    }


    @Test
    public void normal1() {
        Folyam<Integer> f = Folyam.range(1, 5).publish().autoConnect();

        TestConsumer<Integer> tc1 = f.test();

        TestConsumer<Integer> tc2 = f.test();

        tc1.assertResult(1, 2, 3, 4, 5);
        tc2.assertResult();
    }

    @Test
    public void disconnect() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        SequentialAutoDisposable sd = new SequentialAutoDisposable();

        Folyam<Integer> f = dp.publish().autoConnect(2, sd::replace);

        assertFalse(dp.hasSubscribers());

        TestConsumer<Integer> tc1 = f.test();

        assertFalse(dp.hasSubscribers());

        TestConsumer<Integer> tc2 = f.test();

        assertTrue(dp.hasSubscribers());

        sd.close();

        assertFalse(dp.hasSubscribers());

        tc1.assertFailure(CancellationException.class);
        tc2.assertFailure(CancellationException.class);
    }

}
