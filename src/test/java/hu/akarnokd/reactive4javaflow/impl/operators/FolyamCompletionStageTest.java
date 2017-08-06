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
import java.util.concurrent.CompletableFuture;

public class FolyamCompletionStageTest {

    @Test
    public void standard() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(1);
        TestHelper.assertResult(Folyam.fromCompletionStage(cf), 1);
    }

    @Test
    public void error() {
        CompletableFuture<Integer> cf = CompletableFuture.failedFuture(new IOException());

        Folyam.fromCompletionStage(cf)
                .test()
                .assertFailure(IOException.class);
    }


    @Test
    public void empty() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(null);

        Folyam.fromCompletionStage(cf)
                .test()
                .assertResult();
    }

    @Test
    public void cancel() {
        CompletableFuture<Integer> cf = new CompletableFuture<>();

        TestConsumer<Integer> ts = Folyam.fromCompletionStage(cf)
                .test()
                .cancel();

        cf.complete(1);

        ts.assertEmpty();
    }
}
