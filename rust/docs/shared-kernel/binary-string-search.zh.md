# 二进制字符串比较和查找

TiDB 固定在 c7b93f958531856aacc00986b6f66e1e9ec4791a，SDK 固定在 cf7dc12be2036240972972d925a5b9e49a968c98。

STRCMP、INSTR、双参数 LOCATE 和列列表 FIND_IN_SET 复用原有 SDK 内核。仅接纳二进制排序规则和类型化字节叶节点。实际 SQL 核对了尾空格、非 UTF-8、NUL、字节索引、空列表、空成员和 SQL NULL。可缓存的常量列表及需转换的其他排序规则仍走原路径。

81 项共享表达式、49 项 SQL、限定 Clippy 和根 lint 通过。纯字节调用使用现有分批执行，性能未测。
