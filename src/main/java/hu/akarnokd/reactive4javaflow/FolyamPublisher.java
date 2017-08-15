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

package hu.akarnokd.reactive4javaflow;

import java.util.concurrent.Flow;

/**
 * A common super interface of {@link Folyam} and {@link Esetleg} that
 * allows direct subscription via a {@link FolyamSubscriber}.
 * @param <T> the item type of the flow
 */
public interface FolyamPublisher<T> extends Flow.Publisher<T> {

    /**
     * Subscribe with the library-specific {@link FolyamSubscriber}
     * that is required to not violate the Reactive-Streams specification
     * in exchange of less overhead.
     * @param subscriber the subscriber to use
     */
    void subscribe(FolyamSubscriber<? super T> subscriber);
}
