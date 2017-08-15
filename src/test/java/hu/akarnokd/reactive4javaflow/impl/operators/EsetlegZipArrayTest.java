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
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import hu.akarnokd.reactive4javaflow.impl.FailingFusedSubscription;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Flow;

public class EsetlegZipArrayTest {

    @Test
    public void normal() {
        Esetleg.zip(Esetleg.just(1), Esetleg.just(1), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1");
    }


    @Test
    public void with() {
        Esetleg.just(1).zipWith(Esetleg.just(1), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1");
    }

    @Test
    public void normalConditional() {
        Esetleg.zip(Esetleg.just(1), Esetleg.just(1), (a, b) -> a + "-" + b)
                .filter(v -> true)
                .test()
                .assertResult("1-1");
    }


    @Test
    public void normalDelayError() {
        Esetleg.zipDelayError(Esetleg.just(1), Esetleg.just(1), (a, b) -> a + "-" + b)
                .test()
                .assertResult("1-1");
    }

    @Test
    public void normalArray() {
        Esetleg.zipArray(Arrays::toString, Esetleg.just(1), Esetleg.just(1))
                .test()
                .assertResult("[1, 1]");
    }


    @Test
    public void normalArrayDelayError() {
        Esetleg.zipArrayDelayError(Arrays::toString, Esetleg.just(1), Esetleg.just(1))
                .test()
                .assertResult("[1, 1]");
    }


    @Test
    public void normalIterable() {
        Esetleg.zip(List.of(Esetleg.just(1), Esetleg.just(1)), Arrays::toString)
                .test()
                .assertResult("[1, 1]");
    }


    @Test
    public void normalIterableDelayError() {
        Esetleg.zipDelayError(List.of(Esetleg.just(1), Esetleg.just(1)), Arrays::toString)
                .test()
                .assertResult("[1, 1]");
    }

}
