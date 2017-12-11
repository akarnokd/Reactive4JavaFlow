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

package hu.akarnokd.reactive4javaflow.tck;

import hu.akarnokd.reactive4javaflow.processors.SolocastProcessor;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.IdentityFlowProcessorVerification;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.*;

@Test
public class SolocastProcessorTckTest extends IdentityFlowProcessorVerification<Integer> {

    public SolocastProcessorTckTest() {
        super(new TestEnvironment(50));
    }

    @Override
    protected Flow.Publisher<Integer> createFailedFlowPublisher() {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>();
        sp.onError(new IOException());
        return sp;
    }

    @Override
    protected Flow.Processor<Integer, Integer> createIdentityFlowProcessor(int bufferSize) {
        SolocastProcessor<Integer> sp = new SolocastProcessor<>(bufferSize);
        return sp.refCount();
    }

    @Override
    public ExecutorService publisherExecutorService() {
        return Executors.newCachedThreadPool();
    }

    @Override
    public Integer createElement(int element) {
        return element;
    }

    @Override
    public long maxSupportedSubscribers() {
        return 1;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1024;
    }
}
