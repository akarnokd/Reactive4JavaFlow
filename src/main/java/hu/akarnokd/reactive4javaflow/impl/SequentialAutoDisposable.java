package hu.akarnokd.reactive4javaflow.impl;

import hu.akarnokd.reactive4javaflow.functionals.AutoDisposable;

import java.util.concurrent.atomic.AtomicReference;

public final class SequentialAutoDisposable extends AtomicReference<AutoDisposable> implements AutoDisposable {

    public SequentialAutoDisposable() {
    }

    public SequentialAutoDisposable(AutoDisposable d) {
        setRelease(d);
    }

    public boolean replace(AutoDisposable next) {
        return DisposableHelper.replace(this, next);
    }

    public boolean update(AutoDisposable next) {
        return DisposableHelper.update(this, next);
    }

    public boolean isClosed() {
        return getAcquire() == DisposableHelper.DISPOSED;
    }

    @Override
    public void close() {
        DisposableHelper.dispose(this);
    }
}
