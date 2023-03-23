CREATE OR REPLACE FUNCTION uppercase(str text) 
RETURNS text AS $$
    return str.upper()
$$ LANGUAGE plpython3u;

SELECT uppercase('hello world');