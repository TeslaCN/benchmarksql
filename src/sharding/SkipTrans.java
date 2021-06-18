/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * description:this for SkipTrans Class
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021.
 *
 * @author Administrator
 * @version [openGauss_debug 0.0.1 2021/6/17]
 * @since 2021/6/17
 */
public class SkipTrans {
    private static org.apache.log4j.Logger log = Logger.getLogger(SkipTrans.class);
    public static enum Str2Code {
        NEW_ORDER("TT_NEW_ORDER", jTPCCTData.TT_NEW_ORDER),
        PAYMENT("TT_PAYMENT", jTPCCTData.TT_PAYMENT),
        ORDER_STATUS("TT_ORDER_STATUS", jTPCCTData.TT_ORDER_STATUS),
        STOCK_LEVEL("TT_STOCK_LEVEL", jTPCCTData.TT_STOCK_LEVEL),
        DELIVERY("TT_DELIVERY", jTPCCTData.TT_DELIVERY),
        DELIVERY_BG("TT_DELIVERY_BG", jTPCCTData.TT_DELIVERY_BG),
        NONE("TT_NONE", jTPCCTData.TT_NONE);
        public final String str;
        public final int code;
        Str2Code(String str, int code) {
            this.str = str;
            this.code = code;
        }

        public static Str2Code str2Code(String str) {
            for (Str2Code c: values()) {
                if (c.name().equals(str) || c.str.equals(str)) {
                    return c;
                }
            }
            return NONE;
        }
    }
    private static Set<Integer> skipTransTypes = new ConcurrentSkipListSet<>();
    public static void initSkipTransTypes(Properties prop) {
        String types = prop.getProperty("skipTransType", "");
        skipTransTypes.addAll(Arrays.stream(types.split(","))
                .map(str->str.trim())
                .filter(str -> !str.isEmpty())
                .map(Str2Code::str2Code)
                .map(code->code.code)
                .collect(Collectors.toSet())
        );
        log.info("=======================================");
        log.info("skiped types=" + types);
        log.info("skiped int code=" + skipTransTypes);

    }

    public static int wrappedTransType(int transType) {
        if (skipTransTypes.contains(transType)) {
            return Str2Code.NONE.code;
        }
        return transType;
    }
}
