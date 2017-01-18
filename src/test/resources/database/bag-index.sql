create table BagRelation(
    bagId text not null unique,
    base text not null,
    created text not null);

insert into BagRelation values ('70966e8d-b239-4b98-9c27-f0484f857d2a', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2016-07-31T16:01:00.000+01:00');
insert into BagRelation values ('af0955d6-cc0f-476a-a515-31ebf8c12981', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2017-01-01T00:00:00.000+01:00');
insert into BagRelation values ('460a85f0-37e8-400c-98c1-20ce001d3ead', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2017-01-17T14:20:45.000+01:00');
