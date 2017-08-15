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

public class EsetlegSequenceEqualTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).equalsWith(Folyam.range(1, 5))
                , true
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().equalsWith(Folyam.range(1, 5).hide())
                , true
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 4).equalsWith(Folyam.range(1, 5))
                , false
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).equalsWith(Folyam.range(1, 4))
                , false
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Folyam.empty().equalsWith(Folyam.empty())
                , true
        );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Esetleg.sequenceEqual(Esetleg.just(1), Esetleg.just(1))
                , true
        );
    }

    @Test
    public void standard6() {
        TestHelper.assertResult(
                Esetleg.sequenceEqual(Esetleg.just(1), Esetleg.just(2), (a, b) -> a < 3 && b < 3)
                , true
        );
    }

    @Test
    public void firstError() {

        Esetleg.sequenceEqual(Folyam.error(new IOException()), Folyam.range(1, 5))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void firstErrorFused() {
        Esetleg.sequenceEqual(s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC))
                , Folyam.range(1, 5))
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void secondError() {

        Esetleg.sequenceEqual(Folyam.range(1, 5), Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void secondErrorFused() {
        Esetleg.sequenceEqual(
                Folyam.range(1, 5),
                s -> s.onSubscribe(new FailingFusedSubscription(FusedSubscription.SYNC))
                )
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void shortSourcesPrefetch() {
        Esetleg.sequenceEqual(Folyam.range(1, 5), Folyam.range(1, 5), 1)
                .test()
                .assertResult(true);
    }

    @Test
    public void shortSourcesPrefetch2() {
        Folyam.range(1, 5).equalsWith(Folyam.range(1, 5), (a, b) -> a < 10 && b < 10)
                .test()
                .assertResult(true);
    }
    @Test
    public void longSources() {
        Esetleg.sequenceEqual(Folyam.range(1, 1000), Folyam.range(1, 1000), 16)
                .test()
                .assertResult(true);
    }

    @Test
    public void equalsCrash() {
        Folyam.range(1, 5).equalsWith(Folyam.range(6, 5), (a, b) -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void shortSourcesHidden() {
        Esetleg.sequenceEqual(Folyam.range(1, 5).hide(), Folyam.range(1, 5).hide(), 1)
                .test()
                .assertResult(true);
    }

    @Test
    public void cancel() {
        Esetleg.sequenceEqual(Folyam.never(), Esetleg.never())
                .test(0, true, 0)
                .assertEmpty();
    }

    @Test
    public void different() {
        Esetleg.sequenceEqual(Folyam.just(1), Esetleg.just(2))
                .test()
                .assertResult(false);
    }

    @Test
    public void with() {
        Esetleg.just(1).equalsWith(Folyam.just(1))
                .test()
                .assertResult(true);
    }

    @Test
    public void with2() {
        Esetleg.just(1).equalsWith(Folyam.just(2))
                .test()
                .assertResult(false);
    }

    @Test
    public void with3() {
        Esetleg.just(1).equalsWith(Folyam.range(1, 2))
                .test()
                .assertResult(false);
    }
}
