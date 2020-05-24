
SHOW TABLES

SELECT COUNT(*) FROM geographic_identifiers;

SELECT ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'sources'
AND TABLE_NAME = 'products'

INSERT INTO pages(page_url) VALUES ("www.test.com");
