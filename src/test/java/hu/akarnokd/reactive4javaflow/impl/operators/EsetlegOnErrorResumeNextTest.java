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

public class EsetlegOnErrorResumeNextTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .onErrorReturn(6),
                1
        );
    }

    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .onErrorReturn(6),
                1
        );
    }

    @Test
    public void bothError() {
        Esetleg.error(new IOException("First"))
                .onErrorResumeNext(v -> Esetleg.error(new IOException("Second")))
                .test()
                .assertFailureAndMessage(IOException.class, "Second");
    }


    @Test
    public void bothErrorConditional() {
        Esetleg.error(new IOException("First"))
                .onErrorResumeNext(v -> Esetleg.error(new IOException("Second")))
                .filter(v -> true)
                .test()
                .assertFailureAndMessage(IOException.class, "Second");
    }

    @Test
    public void handlerCrash() {
        Esetleg.error(new IOException("First"))
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
        Esetleg.error(new IOException("First"))
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
