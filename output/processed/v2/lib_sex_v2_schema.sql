-- MySQL dump 10.13  Distrib 8.0.45, for Win64 (x86_64)
--
-- Host: localhost    Database: pppp
-- ------------------------------------------------------
-- Server version	8.0.45

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `lib_sex_v2`
--

DROP TABLE IF EXISTS `lib_sex_v2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `lib_sex_v2` (
  `sex_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT 'Sex ID: Primary Key of Sex {"label":"Sex ID","visible":"false","searchable":"false","formtag":"hidden","list":"1","view":"0","add_edit":"1","alias":"id"}',
  `name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'Name: Name of Sex. {"label":"Name","visible":"true","searchable":"true","formtag":"text","rules":"required|min_length[3]","list":"1","view":"1","add_edit":"1","alias":"name"}',
  `code` varchar(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'Code: Code of Sex. {"label":"Code","visible":"true","searchable":"true","formtag":"text","rules":"alpha_dash|min_length[2]","list":"1","view":"1","add_edit":"1"}',
  `dt_inserted` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Date/Time Inserted: Date and Time the record was inserted. {"label":"Date Inserted","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0"}',
  `inserted_by` int unsigned DEFAULT NULL COMMENT 'Inserted By: User ID who inserted the record. {"label":"Inserted By","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0", "joinfldval":"username"}',
  `dt_updated` datetime DEFAULT NULL COMMENT 'Date/Time Last Updated: Date and Time the records was last updated through application. {"label":"Date/Time Last Updated","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0"}',
  `last_updated_by` int unsigned DEFAULT NULL COMMENT 'Last Updated By: User ID who last updated the record. {"label":"Last Updated By","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0", "joinfldval":"username"}',
  `archived` tinyint unsigned NOT NULL DEFAULT '0' COMMENT 'Archived: State if record was archived or deleted. (0-False; 1-True). {"label":"Archived","visible":"false","searchable":"false","formtag":"text","list":"0","view":"0","add_edit":"0","archive":"1","defaultzero":"1","alias":"archived"}',
  `dt_archived` datetime DEFAULT NULL COMMENT 'Date/Time Last Archived: Date and Time the record was last archived. Reference date for physical archiving of records. Set to NULL on default or after record was archived. {"label":"Date/Time Last Archived","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0"}',
  `last_archived_by` int unsigned DEFAULT NULL COMMENT 'Last Archived By: User ID who last archived the record. {"label":"Last Archived By","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0", "joinfldval":"username"}',
  `dt_restored` datetime DEFAULT NULL COMMENT 'Date/Time Last Restored: Date and Time the record was last restored. Set to NULL on default or after record was restored. {"label":"Date/Time Last Restored","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0"}',
  `last_restored_by` int unsigned DEFAULT NULL COMMENT 'Last Restored By: User ID who last restored the record. {"label":"Last Restored By","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0", "joinfldval":"username"}',
  `dt_last_modified` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'Date/Time Last Modified: Date and Time the records was last modified either through front or back end. {"label":"Date/Time Last Modified","visible":"false","searchable":"false","formtag":"text","list":"0","view":"1","add_edit":"0"}',
  PRIMARY KEY (`sex_id`),
  UNIQUE KEY `name` (`name`) USING BTREE,
  UNIQUE KEY `code` (`code`) USING BTREE,
  KEY `inserted_by` (`inserted_by`) USING BTREE,
  KEY `last_updated_by` (`last_updated_by`) USING BTREE,
  KEY `last_archived_by` (`last_archived_by`) USING BTREE,
  KEY `last_restored_by` (`last_restored_by`) USING BTREE,
  FULLTEXT KEY `name_2` (`name`,`code`),
  CONSTRAINT `lib_sex_ibfk_1` FOREIGN KEY (`inserted_by`) REFERENCES `_user` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `lib_sex_ibfk_2` FOREIGN KEY (`last_updated_by`) REFERENCES `_user` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `lib_sex_ibfk_3` FOREIGN KEY (`last_archived_by`) REFERENCES `_user` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `lib_sex_ibfk_4` FOREIGN KEY (`last_restored_by`) REFERENCES `_user` (`user_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Sex: Library of Sex: Reference' ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`appuser_staging`@`%`*/ /*!50003 TRIGGER `lib_sex_before_update` BEFORE UPDATE ON `lib_sex_v2` FOR EACH ROW BEGIN	



	IF NEW.name <> OLD.name THEN

		INSERT INTO _audit_trail 

			(modified_table, modified_field,row_id, prev_value, new_value, func_type, user_id, dt_last_changed) 

		VALUES

			('lib_sex', 'name', NEW.sex_id, OLD.name, NEW.name, '0', NEW.last_updated_by, NEW.dt_updated);

	END IF;



	IF (NEW.code <> OLD.code) OR (NEW.code IS NOT NULL AND OLD.code IS NULL) THEN

		INSERT INTO _audit_trail 

			(modified_table, modified_field,row_id, prev_value, new_value, func_type, user_id, dt_last_changed) 

		VALUES

			('lib_sex', 'code', NEW.sex_id, OLD.code, NEW.code, '0', NEW.last_updated_by, NEW.dt_updated);

	END IF;



	IF NEW.archived <> OLD.archived AND NEW.archived = 1 THEN

		INSERT INTO _audit_trail 

			(modified_table, modified_field,row_id, prev_value, new_value, func_type, user_id, dt_last_changed) 

		VALUES

			('lib_sex', 'archived', NEW.sex_id, OLD.archived, NEW.archived, '1', NEW.last_archived_by, NEW.dt_archived);

	END IF;



	IF NEW.archived <> OLD.archived AND NEW.archived = 0 THEN

		INSERT INTO _audit_trail 

			(modified_table, modified_field,row_id, prev_value, new_value, func_type, user_id, dt_last_changed) 

		VALUES

			('lib_sex', 'archived', NEW.sex_id, OLD.archived, NEW.archived, '0', NEW.last_restored_by, NEW.dt_restored);

	END IF;



END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-04-10 22:42:41
