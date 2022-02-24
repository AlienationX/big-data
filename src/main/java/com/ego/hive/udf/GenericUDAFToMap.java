package com.ego.hive.udf;

/**
 * Example:
 * select
 *  uid,
 *  kv['c1'] as c1,
 *  kv['c2'] as c2,
 *  kv['c3'] as c3
 * from (
 *  select uid, to_map(key, value) kv
 *    from table
 *   group by uid
 * ) t
 */

public class GenericUDAFToMap {
}
