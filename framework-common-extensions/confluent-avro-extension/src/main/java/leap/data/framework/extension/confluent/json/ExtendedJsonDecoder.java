package leap.data.framework.extension.confluent.json;

import com.fasterxml.jackson.core.JsonToken;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
//import org.codehaus.jackson.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class ExtendedJsonDecoder extends JsonDecoder {
    public ExtendedJsonDecoder(Schema schema, InputStream in) throws IOException {
        super(schema, in);
    }

    public ExtendedJsonDecoder(Schema schema, String in) throws IOException {
        super(schema, in);
    }

    @Override
    public int readIndex() throws IOException {
        advance(Symbol.UNION);
        Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

        String label;
        if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
            label = "null";
        } else if (isNullableSingle(a)) {
            label = getNullableSingle(a);
        }else if (in.getCurrentToken() == JsonToken.START_OBJECT && in.nextToken() == JsonToken.FIELD_NAME) {
            label = in.getText();
            in.nextToken();
            parser.pushSymbol(Symbol.UNION_END);
        } else {
            throw error("start-union");
        }
        int n = a.findLabel(label);
        if (n < 0)
            throw new AvroTypeException("Unknown union branch " + label);
        parser.pushSymbol(a.getSymbol(n));
        return n;
    }


    @Override
    public void readNull() throws IOException {
        advance(Symbol.NULL);
        if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
            in.nextToken();
        }
        else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            if (resultStr != "null")
                throw error("null");
            in.nextToken();
        } else {
            throw error("null");
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        advance(Symbol.BOOLEAN);
        JsonToken t = in.getCurrentToken();
        if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
            in.nextToken();
            return t == JsonToken.VALUE_TRUE;
        } else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            in.nextToken();
            return Boolean.parseBoolean(resultStr);
        } else {
            throw error("boolean");
        }
    }

    @Override
    public int readInt() throws IOException {
        advance(Symbol.INT);
        if (in.getCurrentToken().isNumeric()) {
            int result = in.getIntValue();
            in.nextToken();
            return result;
        } else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            in.nextToken();
            return Integer.parseInt(resultStr);
        } else {
            throw error("int");
        }
    }

    @Override
    public long readLong() throws IOException {
        advance(Symbol.LONG);
        if (in.getCurrentToken().isNumeric()) {
            long result = in.getLongValue();
            in.nextToken();
            return result;
        } else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            in.nextToken();
            return Long.parseLong(resultStr);
        } else {
            throw error("long");
        }
    }

    @Override
    public float readFloat() throws IOException {
        advance(Symbol.FLOAT);
        if (in.getCurrentToken().isNumeric()) {
            float result = in.getFloatValue();
            in.nextToken();
            return result;
        } else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            in.nextToken();
            return Float.parseFloat(resultStr);
        } else {
            throw error("float");
        }
    }

    @Override
    public double readDouble() throws IOException {
        advance(Symbol.DOUBLE);
        if (in.getCurrentToken().isNumeric()) {
            double result = in.getDoubleValue();
            in.nextToken();
            return result;
        } else if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            in.nextToken();
            return Double.parseDouble(resultStr);
        } else {
            throw error("double");
        }
    }


    public int readEpoch() throws IOException {
        advance(Symbol.INT);
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            String resultStr = in.getText();
            int result = (int) LocalDate.parse(resultStr, DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay();
            in.nextToken();
            return result;
        } else {
            throw error("string");
        }
    }

    private boolean isNullableSingle(final Symbol.Alternative top) {
        return top.size() == 2 && ("null".equals(top.getLabel(0)) || "null".equals(top.getLabel(1)));
    }

    private String getNullableSingle(final Symbol.Alternative top) {
        final String label = top.getLabel(0);
        return "null".equals(label) ? top.getLabel(1) : label;
    }

}
