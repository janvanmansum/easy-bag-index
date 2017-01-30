CREATE TABLE bag_info (
    bagId CHAR(36) NOT NULL UNIQUE,
    base CHAR(36) NOT NULL,
    created CHAR(29) not null);
-- TODO: replace with TIME WITH TIMEZONE

GRANT INSERT, SELECT, UPDATE, DELETE ON bag_info TO easy_bag_index;