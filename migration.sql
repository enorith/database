DROP table if exists user;
CREATE table if not exists user
(
    id    bigint unsigned auto_increment primary key,
    name  varchar(255) not null,
    email varchar(128) null,
    age   tinyint unsigned
) collate = utf8mb4_unicode_ci;
insert into `user`(`name`, `email`, `age`) values('tom', 'tom@gmail.com', 28), ('tony', 'tony@gmail.com', 22);
DROP table if exists articles;
CREATE table if not exists articles (
    id    bigint unsigned auto_increment primary key,
    title  varchar(255) not null,
    content text null
);
insert into `articles`(`title`, `content`) values('foo', 'awdjawldjlawdawd awdaw wwqe2e12 awe  wawdawdawd'),('bar', 'mki2 12i323 jhw awjkwa awoi we aw');
