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

public class FolyamTakeWhileTest {

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.range(1, 5)
                .takeWhile(v -> true),
                1, 2, 3, 4, 5
        );
    }


    @Test
    public void standard1() {
        TestHelper.assertResult(Folyam.range(1, 5).hide()
                        .takeWhile(v -> true),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard2() {
        Folyam.range(1, 5)
                .takeWhile(v -> v < 4)
                .test()
                .assertResult(1, 2, 3);
    }

    @Test
    public void standard2Conditional() {
        Folyam.range(1, 5)
                .takeWhile(v -> v < 4)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3);
    }


    @Test
    public void standard4() {
        Folyam.range(1, 5)
                .takeWhile(v -> v < 4)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }

    @Test
    public void standard4Conditional() {
        Folyam.range(1, 5)
                .takeWhile(v -> v < 4)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }

    @Test
    public void standard5() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        sp.takeWhile(v -> v < 4)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }

    @Test
    public void standard5Conditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(2);
        sp.onNext(3);
        sp.onNext(4);
        sp.onNext(5);
        sp.onComplete();

        sp.takeWhile(v -> v < 4)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(Folyam.range(1, 5)
                        .takeWhile(v -> false)
        );
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(5,
                f -> f.takeWhile(v -> { throw new IOException(); }),
                IOException.class
                );
    }


    @Test
    public void error2() {
        TestHelper.assertFailureComposed(-1,
                f -> f.takeWhile(v -> true),
                IOException.class
        );
    }

}
