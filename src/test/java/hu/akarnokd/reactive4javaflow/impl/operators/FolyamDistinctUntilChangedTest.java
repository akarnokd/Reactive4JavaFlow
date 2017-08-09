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

public class FolyamDistinctUntilChangedTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 10).distinctUntilChanged()
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 10).hide().distinctUntilChanged()
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 10).distinctUntilChanged(v -> v)
                , 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.fromArray(1, 1, 3, 3, 6, 6, 6, 9).distinctUntilChanged()
                , 1, 3, 6, 9
        );
    }

    @Test
    public void standard3Hidden() {
        TestHelper.assertResult(
                Folyam.fromArray(1, 1, 3, 3, 6, 6, 6, 9).hide().distinctUntilChanged()
                , 1, 3, 6, 9
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.fromArray(1, 1, 3, 3, 6, 6, 6, 9).distinctUntilChanged(v -> v + 1)
                , 1, 3, 6, 9
        );
    }


    @Test
    public void standard4Hidden() {
        TestHelper.assertResult(
                Folyam.fromArray(1, 1, 3, 3, 6, 6, 6, 9).hide().distinctUntilChanged(v -> v + 1)
                , 1, 3, 6, 9
        );
    }


    @Test
    public void error() {
        Folyam.error(new IOException())
                .distinctUntilChanged()
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void errorConditonal() {
        Folyam.error(new IOException())
                .distinctUntilChanged()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void keySelectorCrash() {
        Folyam.range(1, 3)
                .distinctUntilChanged(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void keySelectorCrashConditional() {
        Folyam.range(1, 3)
                .distinctUntilChanged(v -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void comparatorCrash() {
        Folyam.range(1, 3)
                .distinctUntilChanged((v, w) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void comparatorCrashConditional() {
        Folyam.range(1, 3)
                .distinctUntilChanged((v, w) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void selectorComparatorCrash() {
        Folyam.range(1, 3)
                .distinctUntilChanged(v -> v, (v, w) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class, 1);
    }


    @Test
    public void selectorComparatorCrashConditional() {
        Folyam.range(1, 3)
                .distinctUntilChanged(v -> v, (v, w) -> { throw new IOException(); })
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class, 1);
    }

    @Test
    public void asyncSource() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(1);
        sp.onNext(1);
        sp.onComplete();

        sp.distinctUntilChanged()
                .test(2, false, FusedSubscription.ANY)
                .assertResult(1);

    }


    @Test
    public void asyncSourceConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(1);
        sp.onNext(1);
        sp.onComplete();

        sp.distinctUntilChanged()
                .filter(v -> true)
                .test(2, false, FusedSubscription.ANY)
                .assertResult(1);

    }

    @Test
    public void asyncSourceSelector() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(1);
        sp.onNext(1);
        sp.onComplete();

        sp.distinctUntilChanged(v -> v)
                .test(2, false, FusedSubscription.ANY)
                .assertResult(1);

    }

    @Test
    public void asyncSourceSelectorBoundary() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(1);
        sp.onNext(1);
        sp.onComplete();

        sp.distinctUntilChanged(v -> v)
                .test(2, false, FusedSubscription.ANY | FusedSubscription.BOUNDARY)
                .assertResult(1);

    }

    @Test
    public void asyncSourceConditionalSelector() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onNext(1);
        sp.onNext(1);
        sp.onNext(1);
        sp.onComplete();

        sp.distinctUntilChanged(v -> v)
                .filter(v -> true)
                .test(2, false, FusedSubscription.ANY)
                .assertResult(1);
    }
}
