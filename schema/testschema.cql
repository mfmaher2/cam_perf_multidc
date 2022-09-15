DROP KEYSPACE IF EXISTS dcperf;
CREATE KEYSPACE dcperf WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE users (
   userid uuid,
   firstname varchar,
   lastname varchar,
   email text,
   PRIMARY KEY (userid)
);


//test insert
INSERT INTO users(userid, firstname, lastname, email, created_date) values (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'first', 'last', 'fl@test.com');