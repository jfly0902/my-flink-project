package com.yskj.flink.sink;

import com.yskj.flink.entity.DwsMsg;
import com.yskj.flink.util.MysqlClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: xiang.jin
 * @Date: 2020/6/23 16:37
 */
@Slf4j
public class DwsMsgSinkForMysql extends RichSinkFunction<DwsMsg> {

    private Connection connection;
    private PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MysqlClient.getMysqlConnection();
        String sql = buildSql();
        if(connection != null){
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void invoke(DwsMsg value, Context context) throws Exception {
        log.info(" =======> 插入到 mysql 的值：" + value);
        if (ps == null) {
            ps = connection.prepareStatement(buildSql());
        }
        /**
         * talker,room_msg_wxId,pv_value,window_start,window_end
         */
        ps.setString(1,value.getTalker());
        ps.setString(2,value.getWeChatTime());
        ps.setString(3,value.getRoomMsgWxId());
        ps.setString(4,value.getPvValue());
        ps.setString(5,value.getWindowStart());
        ps.setString(6,value.getWindowEnd());
        ps.setString(7,value.getPvValue());
        ps.execute();
        log.info(" ====> DwsMsgSinkForMysql 插入到 dws_room_msg 数据成功");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
    }

    private String buildSql() {
        return "INSERT INTO dws_room_msg(talker,weChat_time,room_msg_wxId,pv_value,window_start,window_end) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE pv_value = ? ";
    }
}
