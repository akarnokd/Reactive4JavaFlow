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

import hu.akarnokd.reactive4javaflow.Folyam;
import org.junit.Test;

import java.util.Optional;

public class ParallelMapOptionalTest {

    @Test
    public void normal() {
        Folyam.range(1, 10)
                .parallel()
                .mapOptional(v -> v % 2 == 0 ? Optional.of(v) :Optional.empty())
                .sequential()
                .test()
                .assertResult(2, 4, 6, 8, 10);

    }


    @Test
    public void normalConditional() {
        Folyam.range(1, 10)
                .parallel()
                .mapOptional(v -> v % 2 == 0 ? Optional.of(v) :Optional.empty())
                .filter(v -> true)
                .sequential()
                .test()
                .assertResult(2, 4, 6, 8, 10);

    }

}
