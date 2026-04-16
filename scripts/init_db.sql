CREATE DATABASE test;

CREATE TABLE test.electricity (
  `datetime` TIMESTAMP(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Holiday_ID` INT32 NOT NULL,
  `nat_demand` FLOAT32,
  `T2M_toc` FLOAT32,
  `QV2M_toc` FLOAT32,
  `TQL_toc` FLOAT32,
  `W2M_toc` FLOAT32,
  `T2M_san` FLOAT32,
  `QV2M_san` FLOAT32,
  `TQL_san` FLOAT32,
  `W2M_san` FLOAT32,
  `T2M_dav` FLOAT32,
  `QV2M_dav` FLOAT32,
  `TQL_dav` FLOAT32,
  `W2M_dav` FLOAT32,
  `holiday` INT32,
  `school` INT32,
  TIMESTAMP KEY(`datetime`)
)
PARTITION BY HASH (`holiday`) PARTITIONS 1
ENGINE=TimeSeries