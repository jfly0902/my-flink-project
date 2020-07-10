package com.yskj.flink.task;


import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Map;




/**
 * @Author: xiang.jin
 * @Date: 2020/4/28 18:20
 */
public class HBaseTest {
    @SneakyThrows
    public static void main(String[] args) throws IOException {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.readTextFile(parameters.get("input")).setParallelism(2);

        final int windowSize = parameters.getInt("window", 10);
        final int slideSize = parameters.getInt("slide", 5);


        SingleOutputStreamOperator<Tuple2> count = input.flatMap(new FlatMapFunction<String, Tuple2>() {
            @Override
            public void flatMap(String s, Collector<Tuple2> out) throws Exception {
                String[] split = s.split(",");
                for (String str :
                        split) {
                    new Tuple2<>(split, 1);
                }
            }
        }).setParallelism(4).slotSharingGroup("flatMap_msg")
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1).setParallelism(3).slotSharingGroup("sum_sg");

        count.print().setParallelism(3);

        env.execute(" TEST JOB");






        /*// 读取元数据
        File file = new File("F://训练数据//flink_metedata//_metadata");
        // 建立数据读取的管道
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        DataInputStream dataInputStream = new DataInputStream(bufferedInputStream);*/

        /*// 通过 Flink 的 Checkpoints 类解析元数据文件
        //Checkpoints.loadCheckpointMetadata(dataInputStream, MetadataSerializer)
        Savepoint savepoint = Checkpoints.loadCheckpointMetadata(dataInputStream,
                //MetadataSerializer.class.getClassLoader());

        // 拿到checkPointId
        long checkpointId = savepoint.getCheckpointId();

        System.out.println(checkpointId);

        // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
        // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
        Collection<OperatorState> operatorStates = savepoint.getOperatorStates();
        for (OperatorState operatorState : operatorStates) {
            System.out.println(operatorState);
            // 当前算子的状态大小为 0 ，表示算子不带状态，直接退出
            if (operatorState.getStateSize() == 0) {
                continue;
            }

            // 遍历当前算子的所有 subtask
            Collection<OperatorSubtaskState> subStates = operatorState.getStates();
            for (OperatorSubtaskState operatorSubtaskState : subStates) {

                // 解析 operatorSubtaskState 的 ManagedKeyedState
                parseManagedKeyedState(operatorSubtaskState);


                // 解析 operatorSubtaskState 的 ManagedOperatorState

                parseManagedOperatorState(operatorSubtaskState);
            }


        }*/




        /*SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        long time = simpleDateFormat.parse("2020-05-12 00:00:00").getTime();
        long time1 = simpleDateFormat.parse("2020-05-11 23:59:55").getTime();
        long time2 = simpleDateFormat.parse("2020-05-11 23:23:58").getTime();

        System.out.println(time);
        System.out.println(time1);
        System.out.println(time2);
        System.out.println(time-time1);
        System.out.println(time1 - time2);*/
        /*
        String data = HBaseClient.getData("stu", "95001", "course", "math");
        System.out.println("返回的数据：{ }" + data);*/
        //getFiled(WxChatHistoryCloud.class);

        /*String str = "{\"0\":\"zhangsan\",\"1\":\"lisi\",\"2\":\"wangwu\",\"3\":\"maliu\"}";
        Map maps = (Map) JSON.parse(str);
        for (Object key : maps.keySet()) {
            System.out.println(key + "--" + maps.get(key));
        }*/

        /*System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.9.2");
        // HBASE 预分区的设计

        PartitionRowKeyManager partitionRowKeyManager = new PartitionRowKeyManager();
        partitionRowKeyManager.setPartition(10);

        byte[][] splitKeys = partitionRowKeyManager.calcSplitKeys();
        for (int i = splitKeys.length - 1; i >= 0; i--) {
            System.out.println(splitKeys[i]);
        }*/


        /**
         * 根据每天20W的数据量来计算，一条数据1k,一天的数据大概事195M
         * 一年的数据大概70G的数据量，3年的数据也就事210G
         * 一个HFile默认的大小是10G,官网建议每个RS 20~200个regions是比较合理的
         * 因此可以将预分区的个数根据10G一个分区，预先分区的个数为21个
         * 因此与分区是000|001|...|020| 21个分区
         * 考虑到统计的时候每天和近7天的数据统计的频率较高
         * 因此使用 wxId + talker + yyyyMM % 21 得到分区的大小然后进行拼接
         * 然后使用Long.Max_value-timeStamp + （Wxid + talker）MD5拼接为后面的数据
         * 使用时间在前面主要是因为 统计天的数据次数比较多，因此根据次方法保证数据的散列和唯一性
         * rowKey的大小最好设计在16个字节上，8字节的整数倍利用操作系统的最佳特性
         * 根据实际设计需求将rowKey的大小设计为24个字节
         * 001_yyyyMMdd_MD5.create().digest((wxId + talker).getBytes());
         * 4+9+11 = 24个字节
         */
        /*StringBuilder rorKey = new StringBuilder();
        String wxId = "0bb3vqu9baz322";
        String talker = "1000426103";
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        String format = simpleDateFormat.format(date);

        int i = (wxId + talker + format).hashCode();

        byte[] md5s = MessageDigest.getInstance("MD5").digest((wxId + talker).getBytes());
        //new

        String s = new BigInteger(1, md5s).toString(16);

        System.out.println(md5s);



        System.out.println(s);


        byte[] digest = MD5.create().digest((wxId + talker).getBytes());

        String s1 = new BigInteger(1, digest).toString(16);
        System.out.println("sdsdfsfgsdfg" + digest.toString());
        System.out.println(s1);
        long l = Long.MAX_VALUE - System.currentTimeMillis();
        System.out.println(l);
        System.out.println(Math.abs(i % 21));*/




    }

    /**
     * 解析 operatorSubtaskState 的 ManagedKeyedState
     * @param operatorSubtaskState operatorSubtaskState
     */

    private static void parseManagedKeyedState(OperatorSubtaskState operatorSubtaskState) {

        StateObjectCollection<KeyedStateHandle> managedKeyedState = operatorSubtaskState.getManagedKeyedState();
        // 遍历当前 subtask 的 KeyedState
        for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
            // 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
            // 因此仅处理 IncrementalRemoteKeyedStateHandle
            if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                // 或缺sharedState里面的数据
                Map<StateHandleID, StreamStateHandle> sharedState =
                        ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
                // 遍历所有的 sst 文件，key 为 sst 文件名，value 为对应的 hdfs 文件 Handle
                for(Map.Entry<StateHandleID,StreamStateHandle> entry:sharedState.entrySet()){
                    // 打印 sst 文件名
                    System.out.println("sstable 文件名：" + entry.getKey());
                    if(entry.getValue() instanceof FileStateHandle) {
                        Path filePath = ((FileStateHandle) entry.getValue()).getFilePath();
                        // 打印 sst 文件对应的 hdfs 文件位置
                        System.out.println("sstable文件对应的hdfs位置:" + filePath.getPath());
                    }
                }
            }

        }

    }

    /**
     * 解析 operatorSubtaskState 的 ManagedOperatorState
     * 注：OperatorState 不支持 Flink 的 增量 Checkpoint，因此本案例可以不解析
     * @param operatorSubtaskState operatorSubtaskState
     */
    private static void parseManagedOperatorState(OperatorSubtaskState operatorSubtaskState) {
        StateObjectCollection<OperatorStateHandle> managedOperatorState = operatorSubtaskState.getManagedOperatorState();
        // 遍历当前 subtask 的 OperatorState
        /*for(OperatorState operatorStateHandle : managedOperatorState) {
            StreamStateHandle delegateState = operatorStateHandle.getDelegateStateHandle();
            if(delegateState instanceof FileStateHandle) {
                Path filePath = ((FileStateHandle) delegateStateHandle).getFilePath();
                System.out.println(filePath.getPath());
            }
        }*/
    }

    /*public static void getFiled(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Method[] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < fields.length; i++) {
            System.out.println(fields[i].getName());
        }
    }*/



}


class PartitionRowKeyManager {

    public static final int DEFAULT_PARTITION_AMOUNT = 20;
    private long currentId = 1;
    private int partition = DEFAULT_PARTITION_AMOUNT;

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] nextId() {
        try {
            long partitionId = currentId % partition;
            return Bytes.add(Bytes.toBytes(partitionId),
                    Bytes.toBytes(currentId));
        } finally {
            currentId++;
        }
    }

    public byte[][] calcSplitKeys() {
        byte[][] splitKeys = new byte[partition - 1][];
        for (int i = 1; i < partition; i++) {
            splitKeys[i - 1] = Bytes.toBytes((long) i);
        }
        return splitKeys;
    }
}