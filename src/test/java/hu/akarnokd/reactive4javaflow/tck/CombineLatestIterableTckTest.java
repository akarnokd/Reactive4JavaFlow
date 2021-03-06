/*
 * Copyright 2017 David Karnok
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package hu.akarnokd.reactive4javaflow.tck;

import java.util.Arrays;
import java.util.concurrent.Flow;

import hu.akarnokd.reactive4javaflow.Folyam;
import hu.akarnokd.reactive4javaflow.functionals.CheckedFunction;
import org.testng.annotations.Test;

@Test
public class CombineLatestIterableTckTest extends BaseTck<Long> {

    @SuppressWarnings("unchecked")
    @Override
    public Flow.Publisher<Long> createFlowPublisher(long elements) {
        return
            Folyam.combineLatest(Arrays.asList(
                    Folyam.just(1L),
                    Folyam.fromIterable(iterate(elements))
                ),
                    a -> (Long)a[0]
            )
        ;
    }
}
