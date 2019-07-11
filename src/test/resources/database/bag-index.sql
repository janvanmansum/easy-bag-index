CREATE TABLE bag_info (
    bagId CHAR(36) NOT NULL PRIMARY KEY,
    base CHAR(36) NOT NULL,
    created CHAR(29) NOT NULL,
    doi VARCHAR(256) NOT NULL
);
