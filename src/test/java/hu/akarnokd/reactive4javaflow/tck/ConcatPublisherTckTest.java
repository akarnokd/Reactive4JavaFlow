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

import hu.akarnokd.reactive4javaflow.Folyam;
import org.testng.annotations.Test;

import java.util.concurrent.Flow;


@Test
public class ConcatPublisherTckTest extends BaseTck<Long> {

    @Override
    public Flow.Publisher<Long> createFlowPublisher(long elements) {
        return
            Folyam.concat(Folyam.fromArray(
                    Folyam.fromIterable(iterate(elements / 2)),
                    Folyam.fromIterable(iterate(elements - elements / 2))
                )
            )
        ;
    }
}
