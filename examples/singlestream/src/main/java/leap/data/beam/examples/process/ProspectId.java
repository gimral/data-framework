package leap.data.beam.examples.process;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.examples.datatypes.ProspectCompany;

public class ProspectId extends LeapDoFn<ProspectCompany, KV<Long, ProspectCompany>> {

    private static final long serialVersionUID = -8651432909357035843L;

    @Override
    protected void innerProcessElement(ProspectCompany element,
            DoFn<ProspectCompany, KV<Long, ProspectCompany>>.ProcessContext c) {
                  c.output(KV.of(element.getId(), element));  
    }
    
}