package leap.data.beam.examples.process;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.ProspectCompany;

public class FlatMapToAggregateProspect extends LeapDoFn<String,ProspectCompany> {

    private static final long serialVersionUID = 1285473094986748770L;

    @Override
    protected void innerProcessElement(String prospectCompany, ProcessContext ctx) {
        ctx.output(new ProspectCompany());      
    }
}