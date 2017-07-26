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

package hu.akarnokd.reactive4javaflow.fused;

import java.util.concurrent.Flow;

public interface FusedSubscription<T> extends Flow.Subscription, FusedQueue<T> {

    default boolean offer(T item) {
        throw new UnsupportedOperationException("Should not be called!");
    }

    int NONE = 0;
    int SYNC = 1;
    int ASYNC = 2;
    int ANY = SYNC | ASYNC;
    int BOUNDARY = 4;

    int requestFusion(int mode);
}
