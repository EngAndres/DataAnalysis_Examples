CREATE TABLE people (
    id serial NOT NULL,
    "doc" varchar(20) unique NOT NULL,
    "name" varchar(50) NOT NULL,
    "age" int NOT NULL,
    "college" varchar(100) NOT NULL
);

insert into people ("doc", "name", "age", "college") values ('1234', 'Pepito', '32', 'Harvard');
insert into people ("doc", "name", "age", "college") values ('5678', 'Juanita', '45', 'Stanford');
insert into people ("doc", "name", "age", "college") values ('4321', 'Monchito', '35', 'MIT');
insert into people ("doc", "name", "age", "college") values ('9876', 'Sutanita', '25', 'Cambridge');

select id as id_persona, doc as documento, name AS nombre, age AS edad, college AS universidad 
from people order by doc;