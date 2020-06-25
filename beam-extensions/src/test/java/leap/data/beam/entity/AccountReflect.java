package leap.data.beam.entity;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@SuppressWarnings("unused")
@DefaultSchema(JavaBeanSchema.class)
public class AccountReflect {
    private Long acid;
    private Double balance;

    public Long getAcid() {
        return acid;
    }

    public void setAcid(Long acid) {
        this.acid = acid;
    }

    public Double getBalance() {
        return balance;
    }

    public void setBalance(Double balance) {
        this.balance = balance;
    }
}
