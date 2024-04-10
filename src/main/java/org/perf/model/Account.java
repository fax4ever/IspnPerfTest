package org.perf.model;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import java.io.Serializable;


public class Account implements Serializable {

    private static final long serialVersionUID = -9117497251699887165L;
    @ProtoField(number = 1)
    public final String accountId;
    @ProtoField(number = 2)
    final String availableBalance;
    @ProtoField(number = 3)
    final String reserveBalance;
    @ProtoField(number = 4)
    final String updateUETR;

    @ProtoFactory
    public Account(String accountId, String availableBalance, String reserveBalance, String updateUETR) {
        super();
        this.accountId = accountId;
        this.availableBalance = availableBalance;
        this.reserveBalance = reserveBalance;
        this.updateUETR = updateUETR;
    }

}
