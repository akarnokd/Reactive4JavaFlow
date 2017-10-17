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
 * Allows transforming the entire Folyam at assembly time into another flow with
 * possibly different element type by composing operators on the current Folyam
 * on which the compose() operator is applied to.
 * @param <T> The upstream Folyam's value type
 * @param <R> the resulting Folyam's value type
 * @since 0.1.3
 */
public interface FolyamTransformer<T, R> {
    /**
     * The function called at assembly time to produce a Flow.Publisher in
     * response to the current upstream Folyam.
     * @param upstream the upstream Folyam to transform or compose onto
     * @return the Folyam type to use after compose()
     */
    Folyam<R> apply(Folyam<T> upstream);
}
