package RelationType;

import java.util.*;

public class Payload implements java.io.Serializable {

    public String type;
    public Object key;
    public List<Object> attribute_value;
    public List<String> attribute_name;

    public Payload(Payload payload) {
        this.type = payload.type;
        this.key = payload.key;
        this.attribute_name = new ArrayList<>(payload.attribute_name);
        this.attribute_value = new ArrayList<>(payload.attribute_value);
    }

    /**
     * @param type   type of the payload
     * @param key   key of the payload
     * @param attribute_name    attribute name of the payload
     * @param attribute_value   attribute value of the payload
     */
    public Payload(String type, Object key, List<String> attribute_name, List<Object> attribute_value) {
        this.type = type;
        this.key = key;
        this.attribute_name = new ArrayList<>(attribute_name);
        this.attribute_value = new ArrayList<>(attribute_value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() == this.getClass()) {
            for (int i = 0; i < this.attribute_name.size(); i++) {
                int index = Arrays.asList(((Payload) obj).attribute_name).indexOf(attribute_name.get(i));
                if (index == -1) return false;
                if (!((Payload) obj).attribute_value.get(index).equals(attribute_value.get(i))) return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(attribute_value);
    }



    public Object getValueByColumnName(String columnName) {
        int index = attribute_name.indexOf(columnName);
        if (index == -1) {
            return null;
        }else {
            return attribute_value.get(index);
        }
    }

    public void setKey(String nextKey) {
        this.key = getValueByColumnName(nextKey);
    }

    @Override
    public String toString() {
        return "relationType.Payload{" +
                "type='" + type + '\'' +
                ", key=" + key +
                ", attribute_value=" + attribute_value +
                ", attribute_name=" + attribute_name +
                '}';
    }
}