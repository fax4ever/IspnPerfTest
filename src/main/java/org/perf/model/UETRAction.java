package org.perf.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import java.io.Serializable;


@Indexed
public class UETRAction implements Serializable {

    private static final long serialVersionUID = 1434254484114366251L;
    @ProtoField(number = 1)
    @Basic
    String fromAccountId;

    @ProtoField(number = 2)
    @Basic
    String toAccountId;

    @ProtoField(number = 3)
    String uetr;

    @ProtoField(number = 4)
    @Basic(projectable = true)
    String action;

    @ProtoField(number = 5)
    @Basic(projectable = true)
    String amount;

    public void setUetr(String uetr) {
        this.uetr = uetr;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getUetr() {
        return uetr;
    }

    public String getAction() {
        return action;
    }

    public String getAmount() {
        return amount;
    }

    public String getFromAccountId() {
        return fromAccountId;
    }

    public void setFromAccountId(String fromAccountId) {
        this.fromAccountId = fromAccountId;
    }

    public String getToAccountId() {
        return toAccountId;
    }

    public void setToAccountId(String toAccountId) {
        this.toAccountId = toAccountId;
    }

    @ProtoFactory
    public UETRAction(String fromAccountId, String toAccountId, String uetr, String action, String amount) {
        super();
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.uetr = uetr;
        this.action = action;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "UETRAction [fromAccountId=" + fromAccountId + ", toAccountId=" + toAccountId + ",  uetr=" + uetr + ", action=" + action + ", amount=" + amount
                + "]";
    }

}
