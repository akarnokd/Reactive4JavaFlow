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
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FolyamOnBackpressureBufferDropTest {

    @Test
    public void normalNewest() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalNewestFused() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalNewestConditional() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalOldest() {
        Folyam.range(1, 5)
                .onBackpressureDropOldest(10)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalOldestFused() {
        Folyam.range(1, 5)
                .onBackpressureDropOldest(10)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY)
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normalOldestConditional() {
        Folyam.range(1, 5)
                .onBackpressureDropOldest(10)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void noDropNewest() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureDropNewest(10, list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }


    @Test
    public void noDropOldest() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureDropOldest(10, list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.onBackpressureDropNewest(10),
                IOException.class);
    }

    @Test
    public void errorNewestBackpressured() {
        Folyam.error(new IOException())
                .onBackpressureDropNewest(10)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorNewestBackpressuredConditional() {
        Folyam.error(new IOException())
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOldestBackpressured() {
        Folyam.error(new IOException())
                .onBackpressureDropOldest(10)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void errorOldestBackpressuredConditional() {
        Folyam.error(new IOException())
                .onBackpressureDropOldest(10)
                .filter(v -> true)
                .test(0)
                .assertFailure(IOException.class);
    }

    @Test
    public void empty() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .test()
                .assertResult();
    }

    @Test
    public void emptyConditional() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .test()
                .assertResult();
    }


    @Test
    public void emptyBackpressured() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .test(0)
                .assertResult();
    }

    @Test
    public void emptyConditionalBackpressured() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .test(0)
                .assertResult();
    }

    @Test
    public void emptyFused() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .test(0, false, FusedSubscription.ANY)
                .assertResult();
    }

    @Test
    public void emptyFusedConditional() {
        Folyam.empty()
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .test(0, false, FusedSubscription.ANY)
                .assertResult();
    }

    @Test
    public void onDropCrash() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(1, h -> { throw new IOException(); })
                .test(0)
                .assertEmpty()
                .requestMore(2)
                .assertFailure(IOException.class, 2);
    }

    @Test
    public void dropNewest() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureDropNewest(2, list::add)
                .test(0)
                .assertEmpty()
                .requestMore(2)
                .assertResult(1, 5);

        assertEquals(list.toString(), Arrays.asList(2, 3, 4), list);
    }


    @Test
    public void dropOldest() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureDropOldest(2, list::add)
                .test(0)
                .assertEmpty()
                .requestMore(2)
                .assertResult(4, 5);

        assertEquals(list.toString(), Arrays.asList(1, 2, 3), list);
    }

    @Test
    public void take() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .take(3)
                .test()
                .assertResult(1, 2, 3);
    }

    @Test
    public void takeConditional() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .take(3)
                .test()
                .assertResult(1, 2, 3);
    }


    @Test
    public void takeBackpressured() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .take(3)
                .test(3)
                .assertResult(1, 2, 3);
    }

    @Test
    public void takeConditionalBackpressured() {
        Folyam.range(1, 5)
                .onBackpressureDropNewest(10)
                .filter(v -> true)
                .take(3)
                .test(3)
                .assertResult(1, 2, 3);
    }

    @Test
    public void cancel() {
        Folyam.never()
                .onBackpressureDropOldest(10)
                .test(0)
                .cancel()
                .assertEmpty();
    }

    @Test
    public void cancelConditional() {
        Folyam.never()
                .onBackpressureDropOldest(10)
                .filter(v -> true)
                .test(0)
                .cancel()
                .assertEmpty();
    }

    @Test
    public void cancel1() {
        Folyam.never()
                .onBackpressureDropOldest(10)
                .test(0, true, 0)
                .assertEmpty();
    }


    @Test
    public void cancel1Conditional() {
        Folyam.never()
                .onBackpressureDropOldest(10)
                .filter(v -> true)
                .test(0, true, 0)
                .assertEmpty();
    }
}
