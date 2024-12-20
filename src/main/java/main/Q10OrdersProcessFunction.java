package main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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

public class Q10OrdersProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload>{
    String outGoKey = "ORDERKEY";
    ValueState<Set<Payload>> orderState;
    ValueState<Integer> orderNumAlive;
    ValueState<Payload> orderLatestAlive;
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void open(Configuration parameter) throws Exception{
        TypeInformation<Set<Payload>> typeInfo = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> stateDescriptor = new ValueStateDescriptor<>(
                "Q10OrdersProFun: state", typeInfo);
        orderState = getRuntimeContext().getState(stateDescriptor);

        ValueStateDescriptor<Integer> numDescriptor = new ValueStateDescriptor<>(
                "Q10OrdersProFun: num", Integer.class);
        orderNumAlive = getRuntimeContext().getState(numDescriptor);

        ValueStateDescriptor<Payload> latestDescriptor = new ValueStateDescriptor<>(
                "Q10CustomerProFun: latest", Payload.class);
        orderLatestAlive = getRuntimeContext().getState(latestDescriptor);
    }

    private boolean validTuple(Payload data_in) throws ParseException {
        Date startD = format.parse("1993-10-01");
        Date endD = format.parse("1994-01-01");
        return (!((Date) data_in.getValueByColumnName("O_ORDERDATE")).before(startD)) && (((Date) data_in.getValueByColumnName("O_ORDERDATE")).before(endD));
    }

    private void initializeState() throws Exception {
        if (orderState.value() == null) {
            orderState.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            orderNumAlive.update(0);
            orderLatestAlive.update(null);
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
            Set<Payload> set = orderState.value();
            orderLatestAlive.update(payload);
            orderNumAlive.update(orderNumAlive.value() + 1);

            for (Payload connectPayload : set) {
                collectPayload(payload, connectPayload, collector);
            }

        } else {
            orderNumAlive.update(orderNumAlive.value() - 1);

            Set<Payload> set = orderState.value();

            for (Payload connectPayload : set) {
                collectPayload(payload, connectPayload, collector);
            }
            orderLatestAlive.update(null);
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
                if (orderNumAlive.value() == 1) {
                    if (orderState.value().add(temp)) {
                        payload.type = "Alive";
                        collectPayload(payload, orderLatestAlive.value(), collector);
                    }
                }else {
                    orderState.value().add(temp);
                }
            }
        }else if(payload.type.equals("Delete")){
                if(orderNumAlive.value() == 1){
                    if(orderState.value().remove(temp)){
                        payload.type = "Dead";
                        collectPayload(payload, orderLatestAlive.value(), collector);
                    }
                }else{
                    orderState.value().remove(temp);
                }
        }
    }
}
