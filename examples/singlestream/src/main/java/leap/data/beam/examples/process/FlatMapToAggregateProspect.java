package leap.data.beam.examples.process;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.ProspectCompany;
import leap.data.beam.examples.util.Convertor;

public class FlatMapToAggregateProspect extends LeapDoFn<String, ProspectCompany> {

    private static final long serialVersionUID = 1285473094986748770L;

    @Override
    protected void innerProcessElement(String prospectCompany, ProcessContext ctx) {
        ProspectCompany pc;
        try {
            pc = Convertor.convertToProspectCompany(prospectCompany);
            ctx.output(pc); 
        } catch (Exception e) {
            // TODO Check how to push to DLQ
            e.printStackTrace();
        }
    }
}