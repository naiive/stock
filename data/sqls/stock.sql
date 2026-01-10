/*

 Source Server Type    : MySQL
 Source Server Version : 90300 (9.3.0)
 Source Host           : localhost:3306
 Source Schema         : stock

 Target Server Type    : MySQL
 Target Server Version : 90300 (9.3.0)
 File Encoding         : 65001

 Date: 10/01/2026 12:04:22
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for asian_quant_stock_daily
-- ----------------------------
DROP TABLE IF EXISTS `asian_quant_stock_daily`;
CREATE TABLE `asian_quant_stock_daily` (
  `code` varchar(6) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '股票代码',
  `date` date NOT NULL COMMENT '交易日期',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '成交量(股)',
  `amount` double DEFAULT NULL COMMENT '成交额',
  `amplitude` double DEFAULT NULL COMMENT '振幅(%)',
  `pct_chg` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `chg` double DEFAULT NULL COMMENT '涨跌额',
  `turnover_rate` double DEFAULT NULL COMMENT '换手率(%)',
  `adjust` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '复权类型:qfq/hfq/none',
  PRIMARY KEY (`code`,`date`,`adjust`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin COMMENT='A股日线行情数据';

-- ----------------------------
-- Table structure for asian_quant_stock_monthly
-- ----------------------------
DROP TABLE IF EXISTS `asian_quant_stock_monthly`;
CREATE TABLE `asian_quant_stock_monthly` (
  `code` varchar(6) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '股票代码',
  `date` date NOT NULL COMMENT '月线归属日期(月末交易日)',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '成交量(股)',
  `amount` double DEFAULT NULL COMMENT '成交额',
  `amplitude` double DEFAULT NULL COMMENT '振幅(%)',
  `pct_chg` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `chg` double DEFAULT NULL COMMENT '涨跌额',
  `turnover_rate` double DEFAULT NULL COMMENT '换手率(%)',
  `adjust` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '复权类型:qfq/hfq/none',
  PRIMARY KEY (`code`,`date`,`adjust`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin COMMENT='A股月线行情数据';

-- ----------------------------
-- Table structure for asian_quant_stock_weekly
-- ----------------------------
DROP TABLE IF EXISTS `asian_quant_stock_weekly`;
CREATE TABLE `asian_quant_stock_weekly` (
  `code` varchar(6) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '股票代码',
  `date` date NOT NULL COMMENT '周线归属日期(周五)',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '成交量(股)',
  `amount` double DEFAULT NULL COMMENT '成交额',
  `amplitude` double DEFAULT NULL COMMENT '振幅(%)',
  `pct_chg` double DEFAULT NULL COMMENT '涨跌幅(%)',
  `chg` double DEFAULT NULL COMMENT '涨跌额',
  `turnover_rate` double DEFAULT NULL COMMENT '换手率(%)',
  `adjust` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '复权类型:qfq/hfq/none',
  PRIMARY KEY (`code`,`date`,`adjust`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin COMMENT='A股周线行情数据';

SET FOREIGN_KEY_CHECKS = 1;
