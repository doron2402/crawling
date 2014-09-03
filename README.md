crawling
========

## SQL Schema
```
DROP TABLE IF EXISTS `records`;

CREATE TABLE `records` (
  `RecordID` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `xfinityID` bigint(36) unsigned DEFAULT NULL,
  `URL` text NOT NULL,
  `name` text,
  `type` varchar(36) DEFAULT NULL,
  `total_view` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`RecordID`),
  KEY `xfinityID` (`xfinityID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```
