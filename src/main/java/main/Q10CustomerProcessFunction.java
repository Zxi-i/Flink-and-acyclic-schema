package main;

import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;


public class Q10CustomerProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    String outGoKey = "CUSTKEY";
    ValueState<Set<Payload>> customerState;
    ValueState<Integer> customerNumAlive;
    ValueState<Payload> customerLatestAlive;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Payload>> typeInfo = TypeInformation.of(new TypeHint<Set<Payload>>() {
        });

        ValueStateDescriptor<Set<Payload>> stateDescriptor = new ValueStateDescriptor<>(
                "Q10CustomerProFun: state", typeInfo);
        customerState = getRuntimeContext().getState(stateDescriptor);

        ValueStateDescriptor<Integer> numDescriptor = new ValueStateDescriptor<>(
                "Q10CustomerProFun: num", Integer.class);
        customerNumAlive = getRuntimeContext().getState(numDescriptor);

        ValueStateDescriptor<Payload> latestDescriptor = new ValueStateDescriptor<>(
                "Q10CustomerProFun: latest", Payload.class);
        customerLatestAlive = getRuntimeContext().getState(latestDescriptor);
    }

    private void initializeState() throws Exception {
        if (customerState.value() == null) {
            customerState.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            customerNumAlive.update(0);
            customerLatestAlive.update(null);
        }
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

    @Override
    public void processElement1(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> collector) throws Exception {
        initializeState();

        if (payload.type.equals("Alive")) {
            Set<Payload> temp_set = customerState.value();
            customerNumAlive.update(customerNumAlive.value() + 1);
            customerLatestAlive.update(payload);

            for (Payload connectPayload : temp_set) {
                collectPayload(payload, connectPayload, collector);
            }
        } else {
            customerNumAlive.update(customerNumAlive.value() - 1);
            Set<Payload> temp_set = customerState.value();
            for (Payload connectPayload : temp_set) {
                collectPayload(payload, connectPayload, collector);
            }
            customerLatestAlive.update(null);
        }
    }

    @Override
    public void processElement2(Payload payload, KeyedCoProcessFunction<Object, Payload, Payload, Payload>.Context context, Collector<Payload> collector) throws Exception {
        initializeState();

        Payload temp = new Payload(payload);
        temp.type = "TEMP";
        temp.key = 0;

        if (payload.type.equals("Insert")) {
            if (customerNumAlive.value() == 1) {
                if (customerState.value().add(temp)) {
                    payload.type = "Alive";
                    collectPayload(payload, customerLatestAlive.value(), collector);
                }
            } else {
                customerState.value().add(temp);
            }
        } else if (payload.type.equals("Delete")) {
            if (customerNumAlive.value() == 1) {
                if (customerState.value().remove(temp)) {
                    payload.type = "Dead";
                    collectPayload(payload, customerLatestAlive.value(), collector);
                }
            } else {
                customerState.value().remove(temp);
            }
        }
    }

}

