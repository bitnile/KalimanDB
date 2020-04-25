package org.bitnile.kalimandb.service.task;

import java.util.concurrent.Callable;

public interface DBOperationTask<T> extends Callable<T> {

}
