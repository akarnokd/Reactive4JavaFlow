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

package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.functionals.*;

import java.util.concurrent.Flow;

public final class FunctionalHelper {

    private FunctionalHelper() {
        throw new IllegalStateException("No instances!");
    }

    public static final CheckedConsumer<Flow.Subscription> REQUEST_UNBOUNDED = new CheckedConsumer<>() {
        @Override
        public void accept(Flow.Subscription s) throws Throwable {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public String toString() {
            return "REQUEST_UNBOUNDED";
        }
    };

    public static final CheckedConsumer<Object> EMPTY_CONSUMER = o -> {
        // deliberately no-op
    };

    public static final CheckedRunnable EMPTY_RUNNABLE = () -> {
        // deliberately no-op
    };
}
