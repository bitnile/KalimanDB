package org.bitnile.kalimandb.common.protocol.body;

import java.util.List;


public class DatabaseServiceRequestArgs {
    private List<Object> args;

    public DatabaseServiceRequestArgs() {
    }

    public List<Object> getArgs() {
        return args;
    }

    public void setArgs(List<Object> args) {
        this.args = args;
    }
}
