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
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EsetlegFlatMapStreamTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapStream(v -> List.of(2).stream()),
                2
        );
    }


    @Test
    public void standard3() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapStream(v -> List.of(2).stream()),
                2
        );
    }

    @Test
    public void standard5() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapStream(v -> List.of().stream())
        );
    }

    @Test
    public void standard7() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapStream(v -> List.of().stream())
        );
    }

    @Test
    public void standard9() {
        TestHelper.assertResult(
                Esetleg.empty().flatMapStream(v -> List.of(1).stream())
        );
    }

    @Test
    public void standard10() {
        TestHelper.assertResult(
                Esetleg.empty().hide().flatMapStream(v -> List.of(1).stream())
        );
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .flatMapStream(v -> List.of(2).stream())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error2() {
        Esetleg.error(new IOException()).hide()
                .flatMapStream(v -> List.of(2).stream())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error3() {
        Esetleg.just(1)
                .flatMapStream(v -> FolyamStreamTest.toStream(new FailingIterable(1, 10, 10)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void error4() {
        Esetleg.just(1)
                .flatMapStream(v -> FolyamStreamTest.toStream(new FailingIterable(10, 1, 10)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void error5() {
        Esetleg.just(1).hide()
                .flatMapStream(v -> FolyamStreamTest.toStream(new FailingIterable(1, 10, 10)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void error6() {
        Esetleg.just(1).hide()
                .flatMapStream(v -> FolyamStreamTest.toStream(new FailingIterable(10, 1, 10)))
                .test()
                .assertFailure(IllegalStateException.class);
    }

    @Test
    public void mapperCrash() {
        Esetleg.just(1)
                .flatMapStream(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperCrash2() {
        Esetleg.just(1).hide()
                .flatMapStream(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void standard11() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapStream(v -> List.of(1, 2, 3, 4, 5).stream()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard12() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapStream(v -> List.of(1, 2, 3, 4, 5).stream()),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard13() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapStream(v -> List.of(1, 2, 3, 4, 5).stream())
                .filter(v -> true),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void closed() {
        int[] counter = { 0 };
        Esetleg.just(1)
                .flatMapStream(v -> List.of(1).stream().onClose(() -> counter[0]++))
                .test()
                .assertResult(1);

        assertEquals(1, counter[0]);
    }
}
