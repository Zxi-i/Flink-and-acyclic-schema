package main;

import RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q10AggregateProcessFunction extends KeyedProcessFunction<Object, Payload, Payload>{
    ValueState<Double> curValue;
    List<String> returnAttrs = Arrays.asList("C_CUSTKEY", "C_NAME", "C_ACCTBAL", "N_NAME",
            "C_ADDRESS", "C_PHONE", "C_COMMENT");
    String aggReName = "revenue";
    String nextKey = "CUSTKEY";

    @Override
    public void open(Configuration parameters) throws Exception{
        ValueStateDescriptor<Double> curValueDescriptor = new ValueStateDescriptor<>(
                "Q10AggProFun: curValue", TypeInformation.of(Double.class));
        curValue = getRuntimeContext().getState(curValueDescriptor);
    }

    @Override
    public void processElement(Payload payload, KeyedProcessFunction<Object, Payload, Payload>.Context context, Collector<Payload> collector) throws Exception {
        // Initialize preValue if it's null
        if (curValue.value() == null) {
            curValue.update(0.0);
        }

        // Calculate delta
        double extendedPrice = (Double) payload.getValueByColumnName("L_EXTENDEDPRICE");
        double discount = (Double) payload.getValueByColumnName("L_DISCOUNT");
        double delta = extendedPrice * (1.0 - discount);

        // Initialize newValue based on the payload type
        double newValue;
        switch (payload.type) {
            case "Add":
                newValue = curValue.value() + delta;
                break;
            case "Sub":
                newValue = curValue.value() - delta;
                break;
            default:
                return;
        }

        // Update the preValue state
        System.out.println(newValue);
        curValue.update(newValue);

        // Prepare the output attributes
        List<Object> attributeValues = new ArrayList<>();
        List<String> attributeNames = new ArrayList<>();

        // Add relevant attribute values and names
        for (String attrName : returnAttrs) {
            attributeValues.add(payload.getValueByColumnName(attrName));
            attributeNames.add(attrName);
        }

        // Add the aggregated value
        attributeValues.add(newValue);
        attributeNames.add(aggReName);

        // Update the payload with the new attributes
        payload.attribute_value = attributeValues;
        payload.attribute_name = attributeNames;
        payload.setKey(nextKey);
        payload.type = "Output";

        // Emit the updated payload
        System.out.println("aggregate: " + payload);
        collector.collect(payload);
    }
}
