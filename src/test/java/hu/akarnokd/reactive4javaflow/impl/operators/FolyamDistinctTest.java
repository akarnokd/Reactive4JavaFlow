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

public class FolyamDistinctTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).distinct()
                , 1, 2, 3, 4, 5
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().distinct()
                , 1, 2, 3, 4, 5
        );
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .distinct()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .distinct()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void keySelectorCrash() {
        Folyam.just(1)
                .distinct(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void collectionProviderCrash() {
        Folyam.just(1)
                .distinct(v -> 1, () -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void normalBackpressured() {
        Folyam.range(1, 5).distinct(v -> 1)
                .test(0)
                .assertEmpty()
                .requestMore(2)
                .assertResult(1);
    }

    @Test
    public void normalConditionalBackpressured() {
        Folyam.range(1, 5).distinct(v -> 1)
                .filter(v -> true)
                .test(0)
                .assertEmpty()
                .requestMore(2)
                .assertResult(1);
    }

    @Test
    public void fusedSync() {
        Folyam.range(1, 10).distinct(v -> v % 3)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }


    @Test
    public void fusedSyncBoundary() {
        Folyam.range(1, 10).distinct(v -> v % 3)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY | FusedSubscription.BOUNDARY)
                .assertFusionMode(FusedSubscription.NONE)
                .assertResult(1, 2, 3);
    }


    @Test
    public void fusedSyncConditional() {
        Folyam.range(1, 10).distinct(v -> v % 3)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }

    @Test
    public void fusedAsync() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        Folyam.range(1, 10).subscribe(sp);

        sp.distinct(v -> v % 3)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }


    @Test
    public void fusedAsyncConditional() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        Folyam.range(1, 10).subscribe(sp);

        sp.distinct(v -> v % 3)
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3);
    }
}
