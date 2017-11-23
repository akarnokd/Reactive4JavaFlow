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

import hu.akarnokd.reactive4javaflow.SchedulerServices;
import hu.akarnokd.reactive4javaflow.processors.CachingProcessor;
import org.testng.annotations.Test;

import java.util.concurrent.Flow;

@Test
public class CachingProcessorSizeBoundAsPublisherTckTest extends BaseTck<Integer> {

    public CachingProcessorSizeBoundAsPublisherTckTest() {
        super(100);
    }

    @Override
    public Flow.Publisher<Integer> createFlowPublisher(final long elements) {
        final CachingProcessor<Integer> pp = new CachingProcessor<>((int)elements + 10);

        SchedulerServices.io().schedule(() -> {
            long start = System.currentTimeMillis();
            while (!pp.hasSubscribers()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    return;
                }

                if (System.currentTimeMillis() - start > 200) {
                    return;
                }
            }

            for (int i = 0; i < elements; i++) {
                pp.onNext(i);
            }
            pp.onComplete();
        });
        return pp;
    }
}
