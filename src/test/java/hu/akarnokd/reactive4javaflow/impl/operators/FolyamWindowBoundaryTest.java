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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FolyamWindowBoundaryTest {

    @Test
    public void normal() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Integer> tc = main.window(boundary).flatMap(v -> v).test(0);

        assertTrue(main.hasSubscribers());
        assertTrue(boundary.hasSubscribers());

        tc.assertEmpty();

        tc.requestMore(3);

        main.onNext(1);

        tc.assertValues(1);

        main.onNext(2);

        main.onNext(3);

        tc.assertValues(1, 2, 3);

        boundary.onNext(100);

        main.onNext(4);

        main.onComplete();

        tc.requestMore(1);

        tc.assertResult(1, 2, 3, 4);

        assertFalse(boundary.hasSubscribers());
    }


    @Test
    public void normalMaxSize() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Integer> tc = main.window(boundary, 2).flatMap(v -> v).test();

        assertTrue(main.hasSubscribers());
        assertTrue(boundary.hasSubscribers());

        tc.assertEmpty();

        tc.requestMore(3);

        main.onNext(1);

        tc.assertValues(1);

        main.onNext(2);

        main.onNext(3);

        tc.assertValues(1, 2, 3);

        boundary.onNext(100);

        main.onNext(4);

        main.onComplete();

        tc.assertResult(1, 2, 3, 4);

        assertFalse(boundary.hasSubscribers());
    }

    @Test
    public void mainError() {
        TestHelper.withErrorTracking(errors -> {
            DirectProcessor<Integer> main = new DirectProcessor<>();
            DirectProcessor<Integer> boundary = new DirectProcessor<>();

            TestConsumer<Integer> tc = main.window(boundary, 2).flatMap(v -> v).test();

            main.onError(new IOException());

            tc.assertFailure(IOException.class);

            assertFalse(boundary.hasSubscribers());

            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void boundaryError() {
        TestHelper.withErrorTracking(errors -> {
            DirectProcessor<Integer> main = new DirectProcessor<>();
            DirectProcessor<Integer> boundary = new DirectProcessor<>();

            TestConsumer<Integer> tc = main.window(boundary, 2).flatMap(v -> v).test();

            boundary.onError(new IOException());

            tc.assertFailure(IOException.class);

            assertFalse(boundary.hasSubscribers());
            TestHelper.assertError(errors, 0, IOException.class);
        });
    }

    @Test
    public void boundaryCompletes() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Integer> tc = main.window(boundary).flatMap(v -> v).test();

        main.onNext(1);
        main.onNext(2);
        main.onNext(3);

        boundary.onComplete();

        assertFalse(main.hasSubscribers());

        tc.assertResult(1, 2, 3);
    }

    @Test
    public void size() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Folyam<Integer>> tc = main.window(boundary, 2).test();

        tc.assertValueCount(1);

        main.onNext(1);
        main.onNext(2);

        tc.assertValueCount(2);

        tc.values().get(0).test().assertResult(1, 2);

        main.onNext(3);
        main.onNext(4);

        tc.values().get(1).test().assertResult(3, 4);

        main.onComplete();

        tc.assertNoErrors()
                .assertComplete();

    }

    @Test
    public void boundaryCompletesImmediately() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Folyam<Integer>> tc = main.window(boundary, 2).test();

        tc.values().get(0).test().cancel();

        boundary.onComplete();

        assertFalse(main.hasSubscribers());

        tc.assertValueCount(1)
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void take1Cancel() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> boundary = new DirectProcessor<>();

        TestConsumer<Folyam<Integer>> tc = new TestConsumer<>() {
            @Override
            public void onNext(Folyam<Integer> item) {
                item.test().cancel();
                cancel();
            }
        };

        main.window(boundary).subscribe(tc);

        tc.assertEmpty();
    }
}