package com.alibaba.datax.plugin.writer.solrwriter;

/**
 * @Date: 2020/8/27
 * @Copyright（C）: 2014-2020 X-Financial Inc.   All rights reserved.
 * 注意：本内容仅限于小赢科技有限责任公司内部传阅，禁止外泄以及用于其他的商业目的。
 */
public enum SolrFieldType {
    ID,
    STRING,
    TEXT,
    KEYWORD,
    LONG,
    DECIMAL,
    INTEGER,
    SHORT,
    BIGINT,
    BYTE,
    DOUBLE,
    FLOAT,
    DATE,
    BOOLEAN,
    BINARY,
    INTEGER_RANGE,
    FLOAT_RANGE,
    LONG_RANGE,
    DOUBLE_RANGE,
    DATE_RANGE,
    GEO_POINT,
    GEO_SHAPE,

    IP,
    COMPLETION,
    TOKEN_COUNT,

    ARRAY,
    OBJECT,
    NESTED;

    public static SolrFieldType getESFieldType(String type) {
        if (type == null) {
            return null;
        }
        for (SolrFieldType f : SolrFieldType.values()) {
            if (f.name().compareTo(type.toUpperCase()) == 0) {
                return f;
            }
        }
        return null;
    }
}
