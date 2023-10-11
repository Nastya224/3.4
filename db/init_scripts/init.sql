ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 100;
CREATE TABLE IF NOT EXISTS public.vacancies (id serial PRIMARY KEY, company VARCHAR(100), vacancy VARCHAR(100), 
                                             skills VARCHAR(1000), meta VARCHAR(100),salary VARCHAR(100), 
                                             date VARCHAR(200), link_vacancy VARCHAR(300), description TEXT);

