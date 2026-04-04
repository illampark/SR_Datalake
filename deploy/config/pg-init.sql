-- SR DataLake — PostgreSQL 초기화
-- docker-entrypoint-initdb.d에 의해 최초 1회 자동 실행

-- sdl_tsdb (TimescaleDB용 별도 DB, 필요 시)
SELECT 'CREATE DATABASE sdl_tsdb OWNER sdl_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sdl_tsdb')\gexec

-- 권한 부여
GRANT ALL PRIVILEGES ON DATABASE sdl TO sdl_user;
