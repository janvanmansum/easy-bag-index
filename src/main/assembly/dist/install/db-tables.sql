CREATE TABLE bag_info (
    bagId CHAR(36) NOT NULL PRIMARY KEY,
    base CHAR(36) NOT NULL,
    created VARCHAR(29) NOT NULL,
    doi VARCHAR(256) NOT NULL);
-- TODO: replace with TIME WITH TIMEZONE

GRANT INSERT, SELECT, UPDATE, DELETE ON bag_info TO easy_bag_index;
