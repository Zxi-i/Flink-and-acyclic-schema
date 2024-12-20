package main;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



public class Q10NationProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    String outGoKey = "NATIONKEY";
    ValueState<Set<Payload>> nationState;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Payload>> typeInformation = TypeInformation.of(new TypeHint<Set<Payload>>() {
        });
        ValueStateDescriptor<Set<Payload>> aliveDescriptor = new ValueStateDescriptor<>(
                "Q10NationProcessFunction" + "Alive", typeInformation);
        nationState = getRuntimeContext().getState(aliveDescriptor);
    }

    public String getOutGoKeyKey() {
        return outGoKey;
    }

    public void processElement(Payload data_in, Context ctx, Collector<Payload> out) throws Exception {
        if (nationState.value() == null) {
            nationState.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }
        Payload temp = new Payload(data_in);
        temp.type = "Temp";
        Set<Payload> valueSet = nationState.value();
        if (data_in.type.equals("Insert")) {
            // Update the nation state
            if (valueSet.add(temp)) {
                data_in.type = "Alive";
                data_in.setKey(outGoKey);
                // Emit the nation payload for further processing
                out.collect(data_in);
            }
        } else{
            if (valueSet.remove(temp)) {
                data_in.type = "Dead"; // Clear the state for the deleted nation
                data_in.setKey(outGoKey); // Mark as deleted
                out.collect(data_in); // Emit deletion notification
                valueSet.remove(temp);
                // valueSet.remove(temp);
            }
        }

    }
}
