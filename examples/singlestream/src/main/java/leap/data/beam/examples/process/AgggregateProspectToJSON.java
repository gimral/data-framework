package leap.data.beam.examples.process;

import org.apache.beam.sdk.transforms.DoFn;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.AggregatedProspectCompany;

public class AgggregateProspectToJSON extends LeapDoFn<AggregatedProspectCompany, String> {

    private static final long serialVersionUID = 123232323434L;

    @Override
    protected void innerProcessElement(AggregatedProspectCompany element,
            DoFn<AggregatedProspectCompany, String>.ProcessContext c) {
        c.output("aaa");

    }

    
}