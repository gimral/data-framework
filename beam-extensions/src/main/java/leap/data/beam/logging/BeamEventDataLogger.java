package leap.data.beam.logging;

import leap.data.framework.core.logging.EventDataLogger;
import org.apache.beam.sdk.values.Row;

public class BeamEventDataLogger extends EventDataLogger {
    @Override
    public void log(Object record) {
        if(record instanceof Row) {
            log((Row) record);
            return;
        }
        super.log(record);
    }

    private void log(Row record){
        //
    }
}
