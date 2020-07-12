package leap.data.beam.examples.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.AggregatedProspectCompany;
import leap.data.beam.examples.datatypes.ProspectCompany;

public class ReduceProspectCo extends LeapDoFn<Iterable<ProspectCompany>, AggregatedProspectCompany> {

    private static final long serialVersionUID = 13232323232L;

    @Override
    protected void innerProcessElement(Iterable<ProspectCompany> element,
            DoFn<Iterable<ProspectCompany>, AggregatedProspectCompany>.ProcessContext c) {
                AggregatedProspectCompany agpc = new AggregatedProspectCompany();
                for (ProspectCompany prospectCompany : element) {
                    agpc.addCompany(prospectCompany);
                }
            c.output(agpc);    
    }

    

      

    
}