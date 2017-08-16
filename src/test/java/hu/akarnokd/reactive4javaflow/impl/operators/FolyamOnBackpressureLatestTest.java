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
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class FolyamOnBackpressureLatestTest {

    @Test
    public void normal() {
        Folyam.range(1, 5)
                .onBackpressureLatest()
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal2() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureLatest(list::add)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }

    @Test
    public void normalBackpressured() {
        Folyam.range(1, 5)
                .onBackpressureLatest()
                .test(3)
                .assertValues(1, 2, 3)
                .assertNotComplete()
                .requestMore(2)
                .assertResult(1, 2, 3, 5);
    }

    @Test
    public void normalBackpressured2() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureLatest(list::add)
                .test(3)
                .assertValues(1, 2, 3)
                .assertNotComplete()
                .requestMore(2)
                .assertResult(1, 2, 3, 5);

        assertEquals(list.toString(), 1, list.size());
        assertEquals(list.toString(), 4, list.get(0).intValue());
    }

    @Test
    public void error() {
        Folyam.error(new IOException())
                .onBackpressureLatest()
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onDropCrash() {
        Folyam.range(1, 5)
                .onBackpressureLatest(v -> { throw new IOException(); })
                .test(0)
                .assertEmpty()
                .requestMore(1)
                .assertFailure(IOException.class, 2);
    }


    @Test
    public void normalConditional() {
        Folyam.range(1, 5)
                .onBackpressureLatest()
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);
    }

    @Test
    public void normal2Conditional() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureLatest(list::add)
                .filter(v -> true)
                .test()
                .assertResult(1, 2, 3, 4, 5);

        assertTrue(list.toString(), list.isEmpty());
    }

    @Test
    public void normalBackpressuredConditional() {
        Folyam.range(1, 5)
                .onBackpressureLatest()
                .filter(v -> true)
                .test(3)
                .assertValues(1, 2, 3)
                .assertNotComplete()
                .requestMore(2)
                .assertResult(1, 2, 3, 5);
    }

    @Test
    public void normalBackpressured2Conditional() {
        List<Integer> list = new ArrayList<>();

        Folyam.range(1, 5)
                .onBackpressureLatest(list::add)
                .filter(v -> true)
                .test(3)
                .assertValues(1, 2, 3)
                .assertNotComplete()
                .requestMore(2)
                .assertResult(1, 2, 3, 5);

        assertEquals(list.toString(), 1, list.size());
        assertEquals(list.toString(), 4, list.get(0).intValue());
    }

    @Test
    public void errorConditional() {
        Folyam.error(new IOException())
                .onBackpressureLatest()
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void onDropCrashConditional() {
        Folyam.range(1, 5)
                .onBackpressureLatest(v -> { throw new IOException(); })
                .filter(v -> true)
                .test(0)
                .assertEmpty()
                .requestMore(1)
                .assertFailure(IOException.class, 2);
    }

    @Test
    public void standard() {
        TestHelper.assertResult(Folyam.just(1).onBackpressureLatest(), 1);
    }

    @Test
    public void cancel() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.onBackpressureLatest().test(0);

        dp.onNext(1);

        tc.cancel();

        assertFalse(dp.hasSubscribers());
    }

    @Test
    public void fusedNormal() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.onBackpressureLatest().test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        dp.onNext(1);
        dp.onComplete();

        assertFalse(dp.hasSubscribers());

        tc.assertResult(1);
    }


    @Test
    public void fusedError() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.onBackpressureLatest().test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        dp.onNext(1);
        dp.onError(new IOException());

        assertFalse(dp.hasSubscribers());

        tc.assertFailure(IOException.class, 1);
    }

    @Test
    public void fusedConditionalNormal() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.onBackpressureLatest()
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        dp.onNext(1);
        dp.onComplete();

        assertFalse(dp.hasSubscribers());

        tc.assertResult(1);
    }


    @Test
    public void fusedConditionalError() {
        DirectProcessor<Integer> dp = new DirectProcessor<>();

        TestConsumer<Integer> tc = dp.onBackpressureLatest()
                .filter(v -> true)
                .test(Long.MAX_VALUE, false, FusedSubscription.ANY);

        dp.onNext(1);
        dp.onError(new IOException());

        assertFalse(dp.hasSubscribers());

        tc.assertFailure(IOException.class, 1);
    }
}
