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

package hu.akarnokd.reactive4javaflow.impl.opeators;

import hu.akarnokd.reactive4javaflow.Esetleg;
import hu.akarnokd.reactive4javaflow.fused.FusedSubscription;
import org.junit.Test;

import java.io.IOException;

public class EsetlegDeferTest {
    @Test
    public void normal() {
        Esetleg<String> f = Esetleg.defer(() -> {
            int[] counter = { 0 };
            return Esetleg.just(1)
                .map(v -> (counter[0]++) + "-" + v);
        });

        f.test().assertResult("0-1");
        f.test().assertResult("0-1");
    }

    @Test
    public void differentReturns() {
        int[] counter = { 0 };
        Esetleg<Integer> f = Esetleg.defer(() -> Esetleg.just(++counter[0]));

        f.test().assertResult(1);
        f.test().assertResult(2);
        f.test().assertResult(3);
    }

    @Test
    public void factoryCrashes() {
        Esetleg.defer(() -> {
            throw new IOException();
        })
        .test()
        .assertFailure(IOException.class);
    }

    @Test
    public void fusionAvailable() {
        Esetleg.defer(() -> Esetleg.just(1))
                .test(5, false, FusedSubscription.ANY)
                .assertFusionMode(FusedSubscription.SYNC)
                .assertResult(1);
    }
}
