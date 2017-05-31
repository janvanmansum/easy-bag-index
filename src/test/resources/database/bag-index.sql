create table bag_info (
    bagId text not null primary key,
    base text not null,
    created text not null,
    doi text not null);

insert into bag_info values ('70966e8d-b239-4b98-9c27-f0484f857d2a', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2016-07-31T16:01:00.000+01:00', '10.5072/dans-a1b-cd2e');
insert into bag_info values ('af0955d6-cc0f-476a-a515-31ebf8c12981', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2017-01-01T00:00:00.000+01:00', '10.5072/dans-f3g-hi45');
insert into bag_info values ('460a85f0-37e8-400c-98c1-20ce001d3ead', '70966e8d-b239-4b98-9c27-f0484f857d2a', '2017-01-17T14:20:45.000+01:00', '10.5072/dans-j6k-lm78');
