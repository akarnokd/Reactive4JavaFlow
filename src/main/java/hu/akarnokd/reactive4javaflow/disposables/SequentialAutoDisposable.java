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

package hu.akarnokd.reactive4javaflow.disposables;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;
import hu.akarnokd.reactive4javaflow.impl.*;

import java.lang.invoke.*;

public final class SequentialAutoDisposable implements AutoDisposable {

    AutoDisposable d;
    static final VarHandle D = VH.find(MethodHandles.lookup(), SequentialAutoDisposable.class, "d", AutoDisposable.class);

    public SequentialAutoDisposable() {
    }

    public SequentialAutoDisposable(AutoDisposable d) {
        D.setRelease(this, d);
    }

    public boolean replace(AutoDisposable next) {
        return DisposableHelper.replace(this, D, next);
    }

    public boolean update(AutoDisposable next) {
        return DisposableHelper.update(this, D, next);
    }

    public AutoDisposable get() {
        return (AutoDisposable)D.getAcquire(this);
    }

    public AutoDisposable getPlain() {
        return d;
    }

    public boolean isClosed() {
        return D.getAcquire(this) == DisposableHelper.CLOSED;
    }

    @Override
    public void close() {
        DisposableHelper.close(this, D);
    }
}
