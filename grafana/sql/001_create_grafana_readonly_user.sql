\set ON_ERROR_STOP on

DO $$
DECLARE
    target_role text := :'role_name';
    target_password text := :'role_password';
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
        EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', target_role, target_password);
    ELSE
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', target_role, target_password);
    END IF;
END
$$;

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'role_name') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'role_name') \gexec

SELECT format('REVOKE ALL ON TABLE public.daily_traffic_summary FROM %I', :'role_name') \gexec
SELECT format('REVOKE ALL ON TABLE public.daily_conversion_funnel FROM %I', :'role_name') \gexec
SELECT format('GRANT SELECT ON TABLE public.daily_traffic_summary TO %I', :'role_name') \gexec
SELECT format('GRANT SELECT ON TABLE public.daily_conversion_funnel TO %I', :'role_name') \gexec

SELECT format('REVOKE ALL ON TABLE public.product_views FROM %I', :'role_name') \gexec
SELECT format('REVOKE ALL ON TABLE public.cart_events FROM %I', :'role_name') \gexec
SELECT format('REVOKE ALL ON TABLE public.orders FROM %I', :'role_name') \gexec
SELECT format('REVOKE ALL ON TABLE public.payments FROM %I', :'role_name') \gexec
SELECT format('REVOKE ALL ON TABLE public.products FROM %I', :'role_name') \gexec
