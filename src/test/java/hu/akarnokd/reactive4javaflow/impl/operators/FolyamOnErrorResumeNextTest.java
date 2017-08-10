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
import hu.akarnokd.reactive4javaflow.errors.CompositeThrowable;
import org.junit.Test;

import java.io.IOException;

public class FolyamOnErrorResumeNextTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                .onErrorReturn(6),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide()
                        .onErrorReturn(6),
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void standard1() {
        TestHelper.assertResult(
                Folyam.range(1, 5).concatWith(Folyam.error(new IOException()))
                        .onErrorReturn(6),
                1, 2, 3, 4, 5, 6
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(Folyam.range(1, 3).concatWith(Folyam.error(new IOException()))
                .onErrorFallback(Folyam.range(4, 3)),
                1, 2, 3, 4, 5, 6);
    }

    @Test
    public void bothError() {
        Folyam.error(new IOException("First"))
                .onErrorResumeNext(v -> Folyam.error(new IOException("Second")))
                .test()
                .assertFailureAndMessage(IOException.class, "Second");
    }


    @Test
    public void bothErrorConditional() {
        Folyam.error(new IOException("First"))
                .onErrorResumeNext(v -> Folyam.error(new IOException("Second")))
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IOException.class, "Second");
    }

    @Test
    public void handlerCrash() {
        Folyam.error(new IOException("First"))
                .onErrorResumeNext(v -> { throw new IOException("Second"); })
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "First");
                    TestHelper.assertError(errors, 1, IOException.class, "Second");
                });
    }


    @Test
    public void handlerCrashConditional() {
        Folyam.error(new IOException("First"))
                .onErrorResumeNext(v -> { throw new IOException("Second"); })
                .filter(v -> true)
                .test()
                .assertFailure(CompositeThrowable.class)
                .assertInnerErrors(errors -> {
                    TestHelper.assertError(errors, 0, IOException.class, "First");
                    TestHelper.assertError(errors, 1, IOException.class, "Second");
                });
    }
}
