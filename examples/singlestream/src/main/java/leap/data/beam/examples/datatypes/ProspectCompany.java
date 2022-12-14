package leap.data.beam.examples.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.joda.time.DateTime;

import leap.data.beam.examples.util.TimeUtil;

@DefaultSchema(JavaBeanSchema.class)
public class ProspectCompany {

    private Long id;
    private String companyName;
    private String tradeLicenseNumber;
    private boolean match;
    private DateTime eventTime;
    private List<Prospect> shareHolders;

    @SchemaCreate
    public ProspectCompany(Long id, String companyName, String tradeLicenseNumber, boolean match, DateTime eventTime) {
        shareHolders = new ArrayList<Prospect>();
        this.id = id;
        this.companyName = companyName;
        this.tradeLicenseNumber = tradeLicenseNumber;
        this.match = match;
        this.eventTime = eventTime;
    }

    public DateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(DateTime eventTime) {
        this.eventTime = eventTime;
    }

    public ProspectCompany() {
        shareHolders = new ArrayList<Prospect>();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public boolean isMatch() {
        return match;
    }

    public void setMatch(boolean match) {
        this.match = match;
    }

    public String getTradeLicenseNumber() {
        return tradeLicenseNumber;
    }

    public void setTradeLicenseNumber(String tradeLicenseNumber) {
        this.tradeLicenseNumber = tradeLicenseNumber;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public void addShareHolder(Prospect shareHolder) {
        this.shareHolders.add(shareHolder);
    }

    public void setShareHolders(List<Prospect> shareHolders) {
        this.shareHolders = shareHolders;
    }
    
    public List<Prospect> getShareHolders(){
        return shareHolders;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(" Id - ").append(id).
        append(" Company Name - ").append(companyName).
        append(" Trade License Number - ").append(tradeLicenseNumber).
        append(" isMatch - ").append(match).
        append(" Event date time - ").append(eventTime.toString(TimeUtil.isoFormatter));
        return sb.toString();
    }
}