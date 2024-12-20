package main;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import RelationType.Payload;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;



public class DataFlow {
    public static final OutputTag<Payload> lineitemTag = new OutputTag<Payload>("lineitem") {};
    public static final OutputTag<Payload> ordersTag = new OutputTag<Payload>("orders") {};
    public static final OutputTag<Payload> customerTag = new OutputTag<Payload>("customer"){};
    public static final OutputTag<Payload> nationTag = new OutputTag<Payload>("nation"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.setParallelism(2);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = "/Users/xi/Downloads/ip_source_data/source_data_3.csv";
        String outputPath = "/Users/xi/Downloads/ip_output_data/output_data_para2_3.csv";
        //String inputPath = parameterTool.get("input");
        //String outputPath = parameterTool.get("output");

        DataStreamSource<String> data = env.readTextFile(inputPath).setParallelism(2);
        SingleOutputStreamOperator<Payload> inputStream = inputStream(data);
        //get side output
        DataStream<Payload> orders = inputStream.getSideOutput(ordersTag);
        DataStream<Payload> lineitem = inputStream.getSideOutput(lineitemTag);
        DataStream<Payload> customer = inputStream.getSideOutput(customerTag);
        DataStream<Payload> nation = inputStream.getSideOutput(nationTag);

        DataStream<Payload> nationS = nation.keyBy(i -> i.key)
                .process(new Q10NationProcessFunction());
        DataStream<Payload> customerS = nationS.connect(customer)
                .keyBy(i -> i.key, i -> i.key)
                .process(new Q10CustomerProcessFunction());
        DataStream<Payload> ordersS = customerS.connect(orders)
                .keyBy(i -> i.key, i -> i.key)
                .process(new Q10OrdersProcessFunction());
        DataStream<Payload> lineitemS = ordersS.connect(lineitem)
                .keyBy(i -> i.key, i -> i.key)
                .process(new Q10LineitemProcessFunction());
        DataStream<Payload> result = lineitemS.keyBy(i -> i.key)
                .process(new Q10AggregateProcessFunction());
        DataStreamSink<Payload> output = result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();
    }

    private static SingleOutputStreamOperator<Payload> inputStream(DataStreamSource<String> data) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        SingleOutputStreamOperator<Payload> restDS = data.process(new ProcessFunction<String, Payload>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Payload> out) throws Exception {
                // Extract header and data cells
                String header = value.substring(0, 7);
                String[] attrs = value.substring(8).split("\\|");

                // Determine action and output based on the header
                if (header.equals("AddLINE") || header.equals("SubLINE")) {
                    handleLineItem(ctx, header, attrs);
                } else if (header.equals("AddORDE") || header.equals("SubORDE")) {
                    handleOrder(ctx, header, attrs);
                } else if (header.equals("AddCUST") || header.equals("SubCUST")) {
                    handleCustomer(ctx, header, attrs);
                } else if (header.equals("AddNATI") || header.equals("SubNATI")) {
                    handleNation(ctx, header, attrs);
                }
            }

            private void handleLineItem(Context ctx, String header, String[] attrs){
                String action = header.equals("AddLINE") ? "Insert" : "Delete";
                ctx.output(lineitemTag, new Payload(action, Long.valueOf(attrs[0]),
                        new ArrayList<>(Arrays.asList("ORDERKEY", "LINENUMBER", "L_EXTENDEDPRICE"
                                , "L_DISCOUNT", "L_RETURNFLAG")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(attrs[0]), Long.valueOf(attrs[3])
                                , Double.valueOf(attrs[5]), Double.valueOf(attrs[6]), attrs[8]))));
            }

            private void handleOrder(Context ctx, String header, String[] attrs) throws Exception {
                String action = header.equals("AddORDE") ? "Insert" : "Delete";
                ctx.output(ordersTag, new Payload(action, Long.valueOf(attrs[1]),
                        new ArrayList<>(Arrays.asList("ORDERKEY", "CUSTKEY", "O_ORDERDATE")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(attrs[0]), Long.valueOf(attrs[1])
                                , format.parse(attrs[4])))));
            }

            private void handleCustomer(Context ctx, String header, String[] attrs){
                String action = header.equals("AddCUST") ? "Insert" : "Delete";
                ctx.output(customerTag, new Payload(action, Long.valueOf(attrs[3]),
                        new ArrayList<>(Arrays.asList("CUSTKEY", "NATIONKEY", "C_NAME", "C_ADDRESS"
                                , "C_PHONE", "C_ACCTBAL",  "C_COMMENT")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(attrs[0]), Long.valueOf(attrs[3])
                                , attrs[1], attrs[2], attrs[4], Double.valueOf(attrs[5]), attrs[7]))));
            }

            private void handleNation(Context ctx, String header, String[] attrs){
                String action = header.equals("AddNATI") ? "Insert" : "Delete";
                ctx.output(nationTag, new Payload(action, Long.valueOf(attrs[0]),
                        new ArrayList<>(Arrays.asList("NATIONKEY", "N_NAME")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(attrs[0]), attrs[1]))));
            }
        }).setParallelism(2);
        return restDS;
    }
}