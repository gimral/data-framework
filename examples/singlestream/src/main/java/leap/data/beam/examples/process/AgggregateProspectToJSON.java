package leap.data.beam.examples.process;

import org.apache.beam.sdk.transforms.DoFn;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.AggregatedProspectCompany;
import leap.data.beam.examples.util.Convertor;

public class AgggregateProspectToJSON extends LeapDoFn<AggregatedProspectCompany, String> {

    private static final long serialVersionUID = 123232323434L;

    @Override
    protected void innerProcessElement(AggregatedProspectCompany element,
            DoFn<AggregatedProspectCompany, String>.ProcessContext c) {
        String agpc;
        try {
            agpc = Convertor.convertAggregatedProspectCompanyToJson(element);
            c.output(agpc);
        } catch (Exception e) {
            // TODO Push to DLQ
            e.printStackTrace();
        }
    }

    
}