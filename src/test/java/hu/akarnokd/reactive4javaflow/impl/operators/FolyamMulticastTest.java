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
import hu.akarnokd.reactive4javaflow.hot.DirectProcessor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FolyamMulticastTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).multicast(Folyam::publish, f -> f),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().multicast(Folyam::publish, f -> f),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.multicast(Folyam::publish, v -> v), IOException.class
        );
    }

    @Test
    public void cancel() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.multicast(Folyam::publish, v -> v).test();

        assertTrue(dp.hasSubscribers());

        tc.cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void cancelDisconnect() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.multicast(Folyam::publish, v -> Folyam.range(1, 5)).test(0);

        assertTrue(dp.hasSubscribers());

        tc.cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void handlerCrash() {
        Folyam.range(1, 5)
                .multicast(Folyam::publish, f -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void disconnectConditional() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.multicast(Folyam::publish, v -> Folyam.range(1, 5))
                .filter(v -> true)
                .test();

        assertFalse(dp.hasSubscribers());

        tc.assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void concat() {
        Folyam.range(1, 5)
                .multicast(Folyam::publish, f -> f.take(3).concatWith(f.takeLast(3)))
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }
}
