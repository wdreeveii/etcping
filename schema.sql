CREATE TABLE `alarms` (
  `ip` varchar(45) NOT NULL,
  `dtStart` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dtEnd` datetime DEFAULT NULL,
  `ticket` varchar(45) DEFAULT NULL,
  `hold` bit(1) NOT NULL DEFAULT b'0',
  PRIMARY KEY (`ip`,`dtStart`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

CREATE TABLE `hosts` (
  `ip` varchar(45) NOT NULL,
  `alias` varchar(500) NOT NULL,
  `primary_alias` bit(1) NOT NULL DEFAULT b'0',
  `dt` datetime NOT NULL,
  PRIMARY KEY (`ip`,`alias`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

CREATE TABLE `pingdata` (
  `dt` datetime NOT NULL,
  `host` varchar(45) NOT NULL,
  `duration` decimal(7,2) NOT NULL,
  PRIMARY KEY (`dt`,`host`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1