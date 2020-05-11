package leap.data.framework.core.serialization;

import java.util.Map;

public class LeapSerializerConfig {
    private final Map<?, ?> props;
    public LeapSerializerConfig(Map<?, ?> props) {
        this.props = props;
    }

    public Map<?, ?> getProps(){
        return props;
    }

    public <T> T getOrDefault(String key, T defaultValue){
        if(props.containsKey(key)){
            return (T)props.get(key);
        }
        return defaultValue;
    }
}
