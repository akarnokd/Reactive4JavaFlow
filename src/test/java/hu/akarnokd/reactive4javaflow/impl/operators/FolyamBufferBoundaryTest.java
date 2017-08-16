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
import hu.akarnokd.reactive4javaflow.processors.DirectProcessor;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class FolyamBufferBoundaryTest {

    @Test
    public void normal() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2).test();

        tc.assertEmpty();

        dp1.onNext(1);
        dp1.onNext(2);

        dp2.onNext(100);

        tc.assertValues(List.of(1, 2));

        dp2.onNext(200);

        tc.assertValues(List.of(1, 2), List.of());

        dp1.onNext(3);
        dp1.onComplete();

        assertFalse(dp2.hasSubscribers());

        tc.assertResult(List.of(1, 2), List.of(), List.of(3));
    }

    @Test
    public void maxSize() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2, 2).test();

        tc.assertEmpty();

        dp1.onNext(1);
        dp1.onNext(2);

        tc.assertValues(List.of(1, 2));

        dp1.onNext(3);
        dp1.onNext(4);

        tc.assertValues(List.of(1, 2), List.of(3, 4));

        dp1.onComplete();

        tc.assertResult(List.of(1, 2), List.of(3, 4));
    }

    @Test
    public void mainError() {
        Folyam.error(new IOException())
                .buffer(Folyam.never())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mainErrorBackpressured() {
        Folyam.error(new IOException())
                .buffer(Folyam.never())
                .test(0L)
                .assertFailure(IOException.class);
    }


    @Test
    public void boundaryError() {
        Folyam.never()
                .buffer(Folyam.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void boundaryErrorBackpressured() {
        Folyam.never()
                .buffer(Folyam.error(new IOException()))
                .test(0L)
                .assertFailure(IOException.class);
    }

    @Test
    public void cancel() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2).test();

        dp1.onNext(1);

        tc.cancel();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void boundaryComplete() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2).test();

        dp1.onNext(1);

        dp2.onComplete();

        tc.assertResult(List.of(1));

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

    }


    @Test
    public void take() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2, 1).take(1).test();

        dp1.onNext(1);

        tc.assertResult(List.of(1));

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());

    }

    @Test
    public void takeRequest1() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2, 1).take(1).test(1);

        dp1.onNext(1);

        tc.assertResult(List.of(1));

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void emptyNoRequest() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2, 1).test(0L);

        dp1.onComplete();

        tc.assertResult();

        assertFalse(dp1.hasSubscribers());
        assertFalse(dp2.hasSubscribers());
    }

    @Test
    public void requestOneByOne() {
        DirectProcessor<Integer> dp1 = new DirectProcessor<>();
        DirectProcessor<Integer> dp2 = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = dp1.buffer(dp2, 1).rebatchRequests(1).test();

        dp1.onNext(1);
        dp1.onNext(2);
        dp1.onNext(3);
        dp2.onNext(100);
        dp1.onComplete();

        tc.assertResult(List.of(1), List.of(2), List.of(3), List.of());
    }

    @Test
    public void collectionSupplierCrash() {
        Folyam.just(1).buffer(Folyam.never(), () -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }
}
