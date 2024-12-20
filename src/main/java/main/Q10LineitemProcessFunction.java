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
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;


public class Q10LineitemProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload>{
    String outGoKey = "ORDERKEY";
    ValueState<Set<Payload>> lineState;
    ValueState<Integer> lineNumAlive;
    ValueState<Payload> lineLatestAlive;

    @Override
    public void open(Configuration parameter) throws Exception{
        TypeInformation<Set<Payload>> typeInfo = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> stateDescriptor = new ValueStateDescriptor<>(
                "Q10LineitemProFun: state", typeInfo);
        lineState = getRuntimeContext().getState(stateDescriptor);

        ValueStateDescriptor<Integer> numDescriptor = new ValueStateDescriptor<>(
                "Q10LineitemProFun: num", Integer.class);
        lineNumAlive = getRuntimeContext().getState(numDescriptor);

        ValueStateDescriptor<Payload> latestDescriptor = new ValueStateDescriptor<>(
                "Q10LineitemProFun: latest", Payload.class);
        lineLatestAlive = getRuntimeContext().getState(latestDescriptor);
    }

    private boolean validTuple(Payload data_in){
        //System.out.println(data_in.getValueByColumnName("L_RETURNFLAG"));
        return data_in.getValueByColumnName("L_RETURNFLAG").equals("R");
    }

    public void collectPayload(Payload previousPayload, Payload connectPayload, Collector<Payload> out) throws Exception {
        Payload temp = new Payload(previousPayload);

        if (connectPayload != null) {
            for (int i = 0; i < connectPayload.attribute_name.size(); i++) {
                if (!temp.attribute_value.contains(connectPayload.attribute_name.get(i))) {
                    temp.attribute_name.add(connectPayload.attribute_name.get(i));
                    temp.attribute_value.add(connectPayload.attribute_value.get(i));
                }
            }
        }
        temp.setKey(outGoKey);
        out.collect(temp);
    }

    private void initializeState() throws Exception {
        if (lineState.value() == null) {
            lineState.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            lineNumAlive.update(0);
            lineLatestAlive.update(null);
        }
    }

    @Override
    public void processElement1(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> collector) throws Exception {
        initializeState();

        if (payload.type.equals("Alive")) {
            Set<Payload> set = lineState.value();
            lineLatestAlive.update(payload);
            lineNumAlive.update(lineNumAlive.value() + 1);

            for (Payload connectPayload : set) {
                // set the type of the payload to add indicates addition (insert new tuple)
                payload.type = "Add";
                collectPayload(payload, connectPayload, collector);
            }

        } else {
            lineNumAlive.update(lineNumAlive.value() - 1);
            Set<Payload> set = lineState.value();
            for (Payload connectPayload : set) {
                // set the type of the payload to minus indicates the subtraction (remove tuple)
                payload.type = "Minus";
                collectPayload(payload, connectPayload, collector);
            }
            lineLatestAlive.update(null);
        }
    }

    @Override
    public void processElement2(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> collector) throws Exception {
        initializeState();

        Payload temp = new Payload(payload);
        temp.type = "TEMP";
        temp.key = 0;

        if(validTuple(payload)) {
            if (payload.type.equals("Insert")) {
                if (lineNumAlive.value() == 1) {
                    if (lineState.value().add(temp)) {
                        payload.type = "Add";
                        collectPayload(payload, lineLatestAlive.value(), collector);
                    }
                }else {
                    lineState.value().add(temp);
                }
            }
        }else if(payload.type.equals("Delete")) {
            if (lineNumAlive.value() == 1) {
                if (lineState.value().remove(temp)) {
                    lineState.value().remove(temp);
                    payload.type = "Minus";
                    collectPayload(payload, lineLatestAlive.value(), collector);
                }
            } else {
                lineState.value().remove(temp);
            }
        }
    }
}
