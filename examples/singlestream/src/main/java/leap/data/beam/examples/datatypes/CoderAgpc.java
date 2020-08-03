package leap.data.beam.examples.datatypes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

public class CoderAgpc extends Coder<AggregatedProspectCompany> {

    private static final long serialVersionUID = 11212123343434L;

    @Override
    public AggregatedProspectCompany decode(InputStream inStream) throws CoderException, IOException {
        ObjectInputStream ois = new ObjectInputStream(inStream);
        try {
            Object object = ois.readObject();
            return (AggregatedProspectCompany)object;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new CoderException(e);
        }
    }

    @Override
    public void encode(AggregatedProspectCompany value, OutputStream outStream) throws CoderException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(value);
        oos.flush();
        oos.close();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        // TODO Auto-generated method stub

    }
    
}
    