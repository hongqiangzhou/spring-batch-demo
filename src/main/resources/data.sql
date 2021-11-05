create table if not exists USER(
    id integer not null,
    username varchar(255) not null,
    password varchar(255) not null,
    age integer not null,
    primary key (id)
);

merge into USER (id, username, password, age) values(1, 'lisi', '123', 23);
merge into USER (id, username, password, age) values(2, 'wangwu', '456', 21);
merge into USER (id, username, password, age) values(3, 'zhaoliu', '666', 26);
merge into USER (id, username, password, age) values(4, 'xiaohong', '777', 24);
merge into USER (id, username, password, age) values(5, 'xiaoming', '999', 27);

create table if not exists CUSTOMER(
    id integer not null,
    first_name varchar(255) not null,
    last_name varchar(255) not null,
    birthday varchar(255) not null,
    primary key (id)
);