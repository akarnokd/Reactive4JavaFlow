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

public class EsetlegRetryWhenTest {

    @Test
    public void standard() {
        TestHelper.assertResult(
                Esetleg.just(1)
                .retryWhen(h -> h),
                1
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Esetleg.just(1).hide()
                        .retryWhen(h -> h),
                1
        );
    }

    @Test
    public void becomeEmpty() {
        TestHelper.assertResult(
                Esetleg.error(new IOException()).retryWhen(h -> {
                    int[] c = { 0 };
                    return h.takeWhile(v -> c[0]++ < 2);
                }));
    }

    @Test
    public void error() {
        TestHelper.assertFailureComposed(-1,
                f -> f.retryWhen(h -> {
                    int[] c = { 0 };
                    return h.takeWhile(v -> {
                        if (c[0]++ < 2) {
                            return true;
                        }
                        throw v;
                    });
                }), IOException.class);
    }

    @Test
    public void handlerCrash() {
        Esetleg.just(1)
                .retryWhen(h -> { throw new IOException(); })
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void immediateError() {
        TestHelper.assertFailureComposed(5,
                f -> f.retryWhen(v -> Esetleg.error(new IOException())), IOException.class);
    }

    @Test
    public void immediateComplete() {
        TestHelper.assertResult(
                Esetleg.just(1)
                        .retryWhen(h -> Esetleg.empty())
        );
    }

}
