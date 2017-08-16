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
import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;
import org.junit.Test;

import java.io.IOException;

public class FolyamSkipWhileTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .skipWhile(v -> v < 1)
                , 1, 2, 3, 4, 5);
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .skipWhile(v -> v < 3)
                , 3, 4, 5);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .skipWhile(v -> v < 6)
                );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(5,
                f -> f.skipWhile(v -> { throw new IOException(); }),
                IOException.class);
    }

    @Test
    public void error2() {
        TestHelper.assertFailureComposed(-1,
                f -> f.skipWhile(v -> false),
                IOException.class);
    }

    @Test
    public void skipAll() {
        Folyam.range(1, 5).hide().skipWhile(v -> true)
                .test()
                .assertResult();
    }

    @Test
    public void skipAllAsyncFused() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onComplete();

        sp.skipWhile(v -> v < 2)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(2);
    }


    @Test
    public void skipAllAsyncFusedConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onComplete();

        sp.skipWhile(v -> v < 2)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(2);
    }

}
