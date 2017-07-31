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

package hu.akarnokd.reactive4javaflow.impl.consumers;

import hu.akarnokd.reactive4javaflow.FolyamPlugins;

import java.util.NoSuchElementException;

public final class BlockingSingleConsumer<T> extends AbstractBlockingConsumer<T> {
    @Override
    public void onNext(T item) {
        if (this.item == null) {
            this.item = item;
        } else {
            cancel();
            onError(new IndexOutOfBoundsException("Too many items"));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (getCount() != 0) {
            this.item = null;
            this.error = throwable;
            countDown();
        } else {
            FolyamPlugins.onError(throwable);
        }
    }

    @Override
    public void onComplete() {
        countDown();
    }
}
