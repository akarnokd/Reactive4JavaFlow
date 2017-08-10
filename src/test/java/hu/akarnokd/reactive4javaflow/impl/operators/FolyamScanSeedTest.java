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

public class FolyamScanSeedTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .scan(() -> 0, (a, b) -> a + b),
                0, 1, 3, 6, 10, 15
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .scan(() -> 0, (a, b) -> a + b),
                0, 1, 3, 6, 10, 15
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1, f -> f
                .scan(() -> 0, (a, b) -> a + b), IOException.class, 0);
    }

    @Test
    public void empty() {
        TestHelper.assertResult(Folyam.<Integer>empty().scan(() -> 0, Integer::sum), 0);
    }

    @Test
    public void initialCrash() {
        Folyam.range(1, 5)
                .scan(() -> { throw new IOException(); }, Integer::sum)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void scannerCrash() {
        Folyam.range(1, 5)
                .scan(() -> 0, (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 0);
    }

    @Test
    public void longSource() {
        Folyam.range(1, 1000)
                .scan(() -> 0, (a, b) -> a + b)
                .test()
                .assertValueCount(1001)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longSourceConditional() {
        Folyam.range(1, 1000)
                .scan(() -> 0, (a, b) -> a + b)
                .filter(v -> true)
                .test()
                .assertValueCount(1001)
                .assertNoErrors()
                .assertComplete();
    }


    @Test
    public void longSourceFused() {
        Folyam.range(1, 1000)
                .scan(() -> 0, (a, b) -> a + b)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertValueCount(1001)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void errorTake() {
        Folyam.<Integer>error(new IOException())
                .scan(() -> 0, Integer::sum)
                .take(2)
                .test(1)
                .assertFailure(IOException.class, 0);
    }

    @Test
    public void emptyTake() {
        Folyam.<Integer>empty()
                .scan(() -> 0, Integer::sum)
                .take(1)
                .test(1)
                .assertResult(0);
    }

    @Test
    public void emptyTakeFused() {
        Folyam.<Integer>empty()
                .scan(() -> 0, Integer::sum)
                .take(1)
                .test(1, false, FusedSubscription.ANY)
                .assertResult(0);
    }


    @Test
    public void emptyTakeConditional() {
        Folyam.<Integer>empty()
                .scan(() -> 0, Integer::sum)
                .take(1)
                .test(1)
                .assertResult(0);
    }

    @Test
    public void emptyTakeConditionalFused() {
        Folyam.<Integer>empty()
                .scan(() -> 0, Integer::sum)
                .filter(v -> true)
                .hide()
                .take(1)
                .test(1, false, FusedSubscription.ANY)
                .assertResult(0);
    }

    @Test
    public void errorTakeConditional() {
        Folyam.<Integer>error(new IOException())
                .scan(() -> 0, Integer::sum)
                .filter(v -> true)
                .take(2)
                .test(1)
                .assertFailure(IOException.class, 0);
    }

    @Test
    public void emptyFusedConditional() {
        Folyam.<Integer>empty()
                .scan(() -> 0, Integer::sum)
                .filter(v -> true)
                .test(1, false, FusedSubscription.ANY)
                .assertResult(0);
    }
}
