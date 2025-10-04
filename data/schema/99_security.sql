-- Create a dedicated read-only role and grant SELECT only
-- Run as superuser (postgres) ONCE.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly_user') THEN
    CREATE ROLE readonly_user LOGIN PASSWORD 'pass';
  END IF;
END$$;

GRANT CONNECT ON DATABASE northwind TO readonly_user;
GRANT USAGE   ON SCHEMA public   TO readonly_user;

-- Current tables
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO readonly_user;

-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;
