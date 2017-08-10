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
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class FolyamWithLatestFromManyTest {

    CheckedFunction<? super Object[], Integer> sum = a -> {
        int sum = 0;
        for (Object o : a) {
            sum += (Integer)o;
        }
        return sum;
    };

    @Test
    public void standard() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFromMany(List.of(Folyam.just(10), Folyam.just(100), Folyam.just(1000)), sum)
                , 1111, 1112, 1113, 1114, 1115
        );
    }


    @Test
    public void standardHidden() {
        TestHelper.assertResult(
                Folyam.range(1, 5)
                        .hide()
                        .withLatestFromMany(List.of(Folyam.just(10), Folyam.just(100), Folyam.just(1000)), sum)
                , 1111, 1112, 1113, 1114, 1115
        );
    }

    @Test
    public void standard2() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFromMany(List.of(Folyam.never()), sum)
        );
    }

    @Test
    public void standard2Hide() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().withLatestFromMany(List.of(Folyam.never()), sum)
        );
    }

    @Test
    public void standard3() {
        TestHelper.assertResult(
                Folyam.range(1, 5).withLatestFromMany(List.of(Folyam.never(), Folyam.empty(), Folyam.never()), sum)
        );
    }

    @Test
    public void standard3HIde() {
        TestHelper.assertResult(
                Folyam.range(1, 5).hide().withLatestFromMany(List.of(Folyam.empty()), sum)
        );
    }

    @Test
    public void mainError() {
        TestHelper.assertFailureComposed(-1,
                f -> f.withLatestFromMany(List.of(Folyam.just(10)), sum),
                IOException.class
        );
    }

    @Test
    public void otherError() {
        TestHelper.assertFailureComposed(5,
                f -> f.withLatestFromMany(List.of(Folyam.<Integer>error(new IOException())), sum),
                IOException.class
        );
    }

    @Test
    public void combinerCrash() {
        TestHelper.assertFailureComposed(5,
                f -> f.withLatestFromMany(List.of(Folyam.just(10)), (a) -> { throw new IOException(); }),
        IOException.class);
    }

    @Test
    public void lotOfOthers() {
        List<Folyam<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(Folyam.never());
        }
        list.add(null);

        Folyam.range(1, 5)
                .withLatestFromMany(list, sum)
                .test()
                .assertFailure(NullPointerException.class);
    }
}
