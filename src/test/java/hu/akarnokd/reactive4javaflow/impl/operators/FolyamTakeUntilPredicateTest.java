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
import hu.akarnokd.reactive4javaflow.hot.SolocastProcessor;
import org.junit.Test;

import java.io.IOException;

public class FolyamTakeUntilPredicateTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .takeUntil(v -> false),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .takeUntil(v -> false),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .takeUntil(v -> v == 3),
                1, 2, 3
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .takeUntil(v -> true),
                1
        );
    }

    @Test
    public void error() {
        Folyam.range(1, 5)
        .takeUntil((Integer v) -> { throw new IOException(); })
        .test()
        .assertFailure(IOException.class, 1);
    }

    @Test
    public void error1() {
        Folyam.range(1, 5)
                .takeUntil((Integer v) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void error2() {
        TestHelper.assertFailureComposed(-1,
                f -> f.takeUntil((Integer v) -> false),
                IOException.class);
    }

    @Test
    public void error3() {
        Folyam.range(1, 5)
                .takeUntil((Integer v) -> { throw new IOException(); })
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }

    @Test
    public void error4() {
        Folyam.range(1, 5)
                .takeUntil((Integer v) -> { throw new IOException(); })
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertFailure(IOException.class);
    }

    @Test
    public void asyncFused() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        sp.takeUntil(v -> v == 3)
        .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
        .assertResult(1, 2, 3);
    }


    @Test
    public void asyncFusedConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        sp.takeUntil(v -> v == 3)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }
}
