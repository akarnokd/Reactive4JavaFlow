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
import static org.junit.Assert.assertTrue;

public class FolyamWindowStartEndTest {

    @Test
    public void normal() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        assertTrue(main.hasSubscribers());
        assertTrue(open.hasSubscribers());

        main.onNext(1);

        open.onNext(100);

        assertTrue(close.hasSubscribers());

        main.onNext(2);
        main.onNext(3);

        close.onNext(1000);

        assertFalse(close.hasSubscribers());

        tc.assertValues(List.of(2, 3));

        open.onNext(200);

        assertTrue(close.hasSubscribers());

        main.onNext(4);
        main.onComplete();

        assertFalse(open.hasSubscribers());
        assertFalse(close.hasSubscribers());

        tc.assertResult(List.of(2, 3), List.of(4));
    }


    @Test
    public void mainError() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        open.onNext(100);

        assertTrue(close.hasSubscribers());

        main.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(open.hasSubscribers());
        assertFalse(close.hasSubscribers());
    }

    @Test
    public void openError() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        open.onNext(100);

        assertTrue(close.hasSubscribers());

        open.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(main.hasSubscribers());
        assertFalse(close.hasSubscribers());
    }

    @Test
    public void closeError() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        open.onNext(100);

        assertTrue(close.hasSubscribers());

        close.onError(new IOException());

        tc.assertFailure(IOException.class);

        assertFalse(main.hasSubscribers());
        assertFalse(open.hasSubscribers());
    }


    @Test
    public void take() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).take(1).flatMap(Folyam::toList).test();

        open.onNext(100);
        close.onNext(1000);

        tc.assertResult(List.of());

        assertFalse(main.hasSubscribers());
        assertFalse(open.hasSubscribers());
        assertFalse(close.hasSubscribers());
    }


    @Test
    public void endFunctionCrash() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();

        TestConsumer<Folyam<Integer>> tc = main.window(open, v -> { throw new IOException(); }).test();

        open.onNext(100);

        tc.assertFailure(IOException.class);

        assertFalse(main.hasSubscribers());
        assertFalse(open.hasSubscribers());
    }

    @Test
    public void startComplete() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        open.onNext(100);

        assertTrue(close.hasSubscribers());

        open.onComplete();

        close.onComplete();

        tc.assertResult(List.of());

        assertFalse(main.hasSubscribers());
        assertFalse(open.hasSubscribers());
        assertFalse(close.hasSubscribers());
    }

    @Test
    public void startCompleteEmpty() {
        DirectProcessor<Integer> main = new DirectProcessor<>();
        DirectProcessor<Integer> open = new DirectProcessor<>();
        DirectProcessor<Integer> close = new DirectProcessor<>();

        TestConsumer<List<Integer>> tc = main.window(open, v -> close).flatMap(Folyam::toList).test();

        open.onComplete();

        tc.assertResult();

        assertFalse(main.hasSubscribers());
        assertFalse(open.hasSubscribers());
        assertFalse(close.hasSubscribers());
    }

}
