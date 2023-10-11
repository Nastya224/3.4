--PostgreSQL
CREATE TABLE IF NOT EXISTS vacancies2
(
id INT, -- Уникальный идентификатор
company String,
vacancy String,
skills String,
meta String,
salary String,
date String,
link_vacancy String,
description String
) ENGINE = PostgreSQL('host.docker.internal:5430', 'test', 'vacancies', 'postgres', 'password');
--MaterializedPostgreSQL
CREATE TABLE IF NOT EXISTS vacancies
(
id INT, -- Уникальный идентификатор
company String,
vacancy String,
skills String,
meta String,
salary String,
date String,
link_vacancy String,
description String
) ENGINE = MaterializedPostgreSQL('host.docker.internal:5430', 'test', 'vacancies', 'postgres', 'password')
ORDER BY (id);

