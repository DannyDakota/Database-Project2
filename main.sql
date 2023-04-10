"Each delivery request has at least one package"

CREATE TRIGGER req_has_package
BEFORE INSERT ON delivery_requests
FOR EACH ROW EXECUTE FUNCTION check_packages_func();

CREATE OR REPLACE FUNCTION check_packages_func() RETURNS TRIGGER
AS $$
begin
  if NOT EXISTS (SELECT 1 FROM packages p WHERE p.request_id = NEW.id) then
    RAISE EXCEPTION 'Each delivery request must have at least one package';
    RETURN NULL;
  else
    RETURN NEW;
  end if; 
end;
$$ LANGUAGE plpgsql;


------------------------------------------------------------------------------------------------------------
"For each delivery request, the IDs of the packages should be consecutive integers starting from 1."

CREATE TRIGGER check_consecutive
BEFORE INSERT ON delivery_requests
FOR EACH ROW EXECUTE FUNCTION check_consecutive_func();

CREATE OR REPLACE FUNCTION check_consecutive_func() 
AS $$
DECLARE max_package_id NUMERIC;
begin
    SELECT MAX(p.package_id) INTO max_package_id
    FROM packages p
    WHERE p.request_id = NEW.id;
    if max_package_id IS NOT NULL AND (max_package_id != COALESCE(NEW.package_id, 0) - 1) then
        RAISE EXCEPTION 'IDs of the packages should be consecutive integers starting from 1';
        RETURN NULL;
    end if;
    if max_package_id IS NULL AND NEW.package_id IS NULL then
        NEW.package_id := 1;
    end if;
    RETURN NEW;
end;
$$ LANGUAGE plpgsql;





------------------------------------------------------------------------------------------------------------
"For each delivery request, the IDs of the unsuccessful pickups should be consecutive integers starting from 1."

CREATE TRIGGER check_pickup_trigger
BEFORE INSERT ON unsuccessful_pickups
FOR EACH ROW EXECUTE FUNCTION check_unsucc_pickup_func();

CREATE OR REPLACE FUNCTION check_unsucc_pickup_func()
AS $$
DECLARE max_unsucc_pickup_id NUMERIC;
begin
    SELECT MAX(pickup_id) INTO max_unsucc_pickup_id
    FROM unsuccessful_pickups u
    WHERE u.request_id =  NEW.request_id;
    if max_unsucc_pickup_id IS NOT NULL AND (max_unsucc_pickup_id != COALESCE(NEW.pickup_id,0) - 1) THEN
        RAISE EXCEPTION 'the IDs of the unsuccessful pickups should be consecutive integers starting from 1';
        RETURN NULL;
    end if;
    if max_unsucc_pickup_id IS NULL AND NEW.pickup_id IS NULL then
        NEW.pickup_id := 1;
    end if;
    RETURN NEW;
end;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------------