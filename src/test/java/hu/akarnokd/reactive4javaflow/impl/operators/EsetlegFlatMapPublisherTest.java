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

public class EsetlegFlatMapPublisherTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapPublisher(v -> Esetleg.just(2)),
                2
        );
    }


    @Test
    public void standard2() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapPublisher(v -> Esetleg.just(2).hide()),
                2
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Esetleg.just(2)),
                2
        );
    }

    @Test
    public void standard4() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Esetleg.just(2).hide()),
                2
        );
    }


    @Test
    public void standard5() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapPublisher(v -> Esetleg.empty())
        );
    }


    @Test
    public void standard6() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapPublisher(v -> Esetleg.empty().hide())
        );
    }

    @Test
    public void standard7() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Esetleg.empty())
        );
    }

    @Test
    public void standard8() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Esetleg.empty().hide())
        );
    }

    @Test
    public void standard9() {
        TestHelper.assertResult(
                Esetleg.empty().flatMapPublisher(v -> Esetleg.just(1))
        );
    }

    @Test
    public void standard10() {
        TestHelper.assertResult(
                Esetleg.empty().hide().flatMapPublisher(v -> Esetleg.just(1))
        );
    }

    @Test
    public void error() {
        Esetleg.error(new IOException())
                .flatMapPublisher(v -> Esetleg.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error2() {
        Esetleg.error(new IOException()).hide()
                .flatMapPublisher(v -> Esetleg.just(2))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error3() {
        Esetleg.just(1)
                .flatMapPublisher(v -> Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error4() {
        Esetleg.just(1)
                .flatMapPublisher(v -> Esetleg.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error5() {
        Esetleg.just(1).hide()
                .flatMapPublisher(v -> Esetleg.error(new IOException()))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error6() {
        Esetleg.just(1).hide()
                .flatMapPublisher(v -> Esetleg.error(new IOException()).hide())
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void error7() {
        Esetleg.just(1).hide()
                .flatMapPublisher(v -> Esetleg.error(new IOException()).hide())
                .filter(v -> true)
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperCrash() {
        Esetleg.just(1)
                .flatMapPublisher(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void mapperCrash2() {
        Esetleg.just(1).hide()
                .flatMapPublisher(v -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void standard11() {
        TestHelper.assertResult(
                Esetleg.just(1).flatMapPublisher(v -> Folyam.range(1, 5)),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard12() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Folyam.range(1, 5)),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard13() {
        TestHelper.assertResult(
                Esetleg.just(1).hide().flatMapPublisher(v -> Folyam.range(1, 5))
                .filter(v -> true),
                1, 2, 3, 4, 5
        );
    }
}
