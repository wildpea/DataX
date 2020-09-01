package com.alibaba.datax.plugin.writer.solrwriter;

import com.alibaba.datax.common.util.Configuration;

/**
 * @Date: 2020/8/27
 * @Copyright（C）: 2014-2020 X-Financial Inc.   All rights reserved.
 * 注意：本内容仅限于小赢科技有限责任公司内部传阅，禁止外泄以及用于其他的商业目的。
 */
public class Key {
    static String getZkQuorum(Configuration conf) {
        return conf.getString("zkQuorum", "");
    }

    static String getCollectionName(Configuration conf) {
        return conf.getString("collectionName", "");
    }

    static Integer getZkConnectTimeout(Configuration conf) {
        return conf.getInt("zkConnectTimeout", 10000);
    }

    static Integer getZkClientTimeout(Configuration conf) {
        return conf.getInt("zkClientTimeout", 10000);
    }

    static String getIdColumn(Configuration conf) {
        return conf.getString("idColumn", "fstruniquekey");
    }

    static int getBatchSize(Configuration conf) {
        return conf.getInt("batchSize", 10000);
    }

    static int getTrySize(Configuration conf) {
        return conf.getInt("trySize", 30);
    }

    static String getSplitter(Configuration conf) {
        return conf.getString("splitter", "\u0001");
    }

    static boolean isIgnoreWriteError(Configuration conf) {
        return conf.getBool("ignoreWriteError", false);
    }

    static boolean isIgnoreParseError(Configuration conf) {
        return conf.getBool("ignoreParseError", true);
    }

}
