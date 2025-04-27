-- Создаем роль metabase
CREATE USER metabase WITH PASSWORD 'metabase';

-- Создаем базу данных metabase
CREATE DATABASE metabase;

-- Назначаем права на базу данных metabase для роли metabase
GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;

-- Даем право на подключение к базе данных
GRANT CONNECT ON DATABASE metabase TO metabase;