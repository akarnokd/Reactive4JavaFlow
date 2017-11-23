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
import hu.akarnokd.reactive4javaflow.fused.FusedDynamicSource;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.junit.Assert.fail;

public class EsetlegFutureTest {

    @Test
    public void normal() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(1);

        Esetleg.fromFuture(cf)
                .test()
                .assertResult(1);
    }

    static <T> Future<T> wrap(CompletableFuture<T> cf) {
        return new Future<>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return cf.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return cf.isCancelled();
            }

            @Override
            public boolean isDone() {
                return cf.isDone();
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                return cf.get();
            }

            @Override
            public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return cf.get(timeout, unit);
            }
        };
    }

    @Test
    public void standard() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(1);
        TestHelper.assertResult(Esetleg.fromFuture(wrap(cf)), 1);
    }


    @Test
    public void standard2() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(1);
        TestHelper.assertResult(Esetleg.fromFuture(wrap(cf), 5, TimeUnit.SECONDS), 1);
    }

    @Test
    public void normal2() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(1);

        Esetleg.fromFuture(wrap(cf))
        .test()
        .assertResult(1);
    }


    @Test
    public void empty() {
        CompletableFuture<Integer> cf = CompletableFuture.completedFuture(null);

        Esetleg.fromFuture(wrap(cf))
                .test()
                .assertResult();
    }

    @Test
    public void error() {
        CompletableFuture<Integer> cf = CompletableFuture.failedFuture(new IOException());

        Esetleg.fromFuture(wrap(cf))
                .test()
                .assertFailure(IOException.class);
    }

    @Test
    public void timeout() {
        CompletableFuture<Integer> cf = new CompletableFuture<>();

        Esetleg.fromFuture(wrap(cf), 10, TimeUnit.MILLISECONDS)
                .test()
                .assertFailure(TimeoutException.class);
    }

    @Test
    public void asyncComplete() {
        CompletableFuture<Integer> cf = new CompletableFuture<>();

        ForkJoinPool.commonPool().submit(() -> {
            Thread.sleep(10);
            cf.complete(1);
            return null;
        });

        TestConsumer<Integer> tc = Esetleg.fromFuture(wrap(cf), 5, TimeUnit.SECONDS)
                .test()
                .assertResult(1);
    }

    @Test
    public void dynamicScalarError() throws Throwable {
        CompletableFuture<Integer> cf = CompletableFuture.failedFuture(new IOException());
        FusedDynamicSource<Integer> src =  (FusedDynamicSource<Integer>)Esetleg.fromFuture(wrap(cf));

        try {
            src.value();
            fail("Should have thrown");
        } catch (IOException ex) {
            // okay
        }
    }

    @Test
    public void dynamicScalarErrorTimeout() throws Throwable {
        CompletableFuture<Integer> cf = CompletableFuture.failedFuture(new IOException());
        FusedDynamicSource<Integer> src =  (FusedDynamicSource<Integer>)Esetleg.fromFuture(wrap(cf), 5, TimeUnit.SECONDS);

        try {
            src.value();
            fail("Should have thrown");
        } catch (IOException ex) {
            // okay
        }
    }

    @Test
    public void dynamicScalarTimeout() throws Throwable {
        CompletableFuture<Integer> cf = new CompletableFuture<>();
        FusedDynamicSource<Integer> src =  (FusedDynamicSource<Integer>)Esetleg.fromFuture(wrap(cf), 10, TimeUnit.MILLISECONDS);

        try {
            src.value();
            fail("Should have thrown");
        } catch (TimeoutException ex) {
            // okay
        }
    }
}
