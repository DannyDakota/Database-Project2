--------------------------------------------------------Delivery_requests related-----------------------------------------------------

--Each delivery request has at least one package

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

--------------------------------------------------------Package related---------------------------------------------------------------

--For each delivery request, the IDs of the packages should be consecutive integers starting from 1.

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


------------------------------------------------------Unsuccessful_pickups related------------------------------------------------

--For each delivery request, the IDs of the unsuccessful pickups should be consecutive integers starting from 1.

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

-----------------------------------------------------------------------------------------------------------------------------------

/*The timestamp of the first unsuccessful pickup should be after the submission_time of the 
corresponding delivery request. In addition, each unsuccessful pickup’s timestamp should be after the 
previous unsuccessful pickup’s timestamp (if any).*/

CREATE TRIGGER check_pickup_timestamp_trigger
BEFORE INSERT ON unsuccessful_pickups
FOR EACH ROW EXECUTE FUNCTION check_pickup_timestamp_func();

CREATE OR REPLACE FUNCTION check_pickup_timestamp_func()
AS $$
DECLARE delivery_request_timestamp TIMESTAMP;
DECLARE first_unsucc_pickup_timestamp TIMESTAMP;
DECLARE last_unsucc_pickup_timestamp TIMESTAMP;
begin
    SELECT d.submission_time into delivery_request_timestamp
    FROM delivery_request d
    WHERE NEW.request_id = d.id;

    SELECT MIN(pickup_time) INTO first_unsucc_pickup_timestamp
    FROM unsuccessful_pickups u 
    WHERE u.request_id = NEW.request_id;

    SELECT MAX(pickup_time) INTO first_unsucc_pickup_timestamp
    FROM unsuccessful_pickups u 
    WHERE u.request_id = NEW.request_id;

    if (first_unsucc_pickup_timestamp < delivery_request_timestamp) then
        RAISE EXCEPTION 'The timestamp of the first unsuccessful pickup should be after the submission_time of the 
            corresponding delivery request';
        return NULL;
    end if;
    if (NEW.pickup_time < last_unsucc_pickup_timestamp) then
    RAISE EXCEPTION 'Each unsuccessful pickup’s timestamp should be after the previous unsuccessful pickup’s timestamp';
        return NULL;
    end if;
    return NEW;
end;
$$ LANGUAGE plpgsql;

------------------------------------------------------------Legs related--------------------------------------------------------------

--For each delivery request, the IDs of the legs should be consecutive integers starting from 1. 

CREATE TRIGGER check_legs_ID_trigger
BEFORE INSERT ON legs
FOR EACH ROW EXECUTE FUNCTION check_legs_ID_func();

CREATE OR REPLACE FUNCTION check_legs_ID_func()
AS $$
DECLARE max_id INT;

BEGIN
    SELECT MAX(leg_id) FROM legs into max_id;
    max_id := max_id + 1;
    IF (NEW.leg_id = max_id) THEN
        RETURN NEW;
    ELSEIF (max_id is NULL) THEN
        NEW.leg_id := 1;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'The IDs of the legs should be consecutive integers';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-------------------------------------------------------------------------------------------------------------------------------------

/*For  each  delivery  request,  the  start  time  of  the  first  leg  should  be  after  the  submission_time  of  the  
delivery request and the timestamp of the last unsuccessful pickup (if any). */

CREATE TRIGGER check_first_leg_start_time_trigger
BEFORE INSERT ON legs
WHEN (NEW.leg_id = 1)
FOR EACH ROW EXECUTE FUNCTION check_first_leg_start_time_func();

CREATE OR REPLACE FUNCTION check_first_leg_start_time_func()
AS $$
DECLARE request_submission_time TIMESTAMP;
DECLARE unsuccessful_pickup_timestamp TIMESTAMP;

BEGIN
    SELECT submission_time FROM delivery_request
    INTO request_submission_time
    WHERE id = NEW.request_id;

    SELECT pickup_time FROM unsuccessful_pickups
    INTO unsuccessful_pickup_timestamp
    WHERE request_id = NEW.request_id;

    IF (NEW.start_time > request_submission_time AND NEW.start_time > COALESCE(unsuccessful_pickup_timestamp, '1900-01-01 00:00:00')) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'For  each  delivery  request,  the  start  time  of  the  first  leg  should  be  after  the  submission_time  of  the  
        delivery request and the timestamp of the last unsuccessful pickup (if any).';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------------------------------------------

/*For each delivery request, a new leg cannot be inserted if its start_time is before the end_time of the 
previous leg, or if the end_time of the previous leg is NULL.*/

CREATE TRIGGER check_legs_start_time_trigger
BEFORE INSERT ON legs
FOR EACH ROW 
WHEN (NEW.leg_id > 1)
EXECUTE FUNCTION check_legs_start_time_func();

CREATE OR REPLACE FUNCTION check_legs_start_time_func()
AS $$
DECLARE previous_leg_end_time TIMESTAMP;
BEGIN
    SELECT end_time FROM legs
    INTO previous_leg_end_time
    WHERE leg_id = NEW.leg_id - 1;

    IF end_time is NULL THEN
        RAISE EXCEPTION 'A new leg cannot be inserted if end time of previous leg cannot be null.';
        RETURN NULL;
    ELSEIF NEW.start_time < previous_leg_end_time THEN
        RAISE EXCEPTION 'A new leg cannot be inserted if it is before the end time of previous leg.';
        RETURN NULL;
    ENDIF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

---------------------------------------------------------------Unsuccessful_deliveries----------------------------------------------------------------------

/*The timestamp of each unsuccessful_delivery should be after the start_time of the corresponding leg.*/

CREATE TRIGGER check_unsuccessful_delivery_timestamp_trigger
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW 
EXECUTE FUNCTION check_unsuccessful_delivery_timestamp_trigger_func();

CREATE OR REPLACE FUNCTION check_unsuccessful_delivery_timestamp_trigger_func()
AS $$
DECLARE leg_start_time TIMESTAMP
BEGIN
    SELECT start_time FROM legs
    INTO leg_start_time
    WHERE leg_id = NEW.leg_id;

    IF NEW.attempt_time <= leg_start_time THEN
        RAISE EXCEPTION 'The timestamp of each unsuccessful delivery should be after the start time of the corresponding leg.';
        RETURN NULL;
    ENDIF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-------------------------------------------------------------------------------------------------------------------------------------
/*For each delivery request, there can be at most three unsuccessful_deliveries.*/

CREATE TRIGGER check_num_of_unsuccessful_deliveries_trigger
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW
EXECUTE FUNCTION check_num_of_unsuccessful_deliveries_func();

CREATE OR REPLACE FUNCTION check_num_of_unsuccessful_deliveries_func()
AS $$
DECLARE curr_num_of_unsuccessful_deliveries INT
BEGIN
    SELECT COUNT(*) FROM unsuccessful_deliveries INTO curr_num_of_unsuccessful_deliveries
    WHERE request_id = NEW.request_id;

    IF curr_num_of_unsuccessful_deliveries >= 3 THEN
        RAISE EXCEPTION 'For each delivery request, there can be at most three unsuccessful_deliveries.';
        RETURN NULL;
    ENDIF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------Cancelled_requests related-----------------------------------------------

/*The cancel time of a cancelled request should be after the submission time of the corresponding delivery request.*/

CREATE TRIGGER check_cancel_time_of_cancelled_request_trigger
BEFORE INSERT ON cancelled_requests
FOR EACH ROW
EXECUTE FUNCTION check_cancel_time_of_cancelled_request_func();

CREATE OR REPLACE FUNCTION check_cancel_time_of_cancelled_request_func()
AS $$
DECLARE delivery_request_submission_time TIMESTAMP
BEGIN
    SELECT submission_time FROM delivery_requests INTO delivery_request_submission_time
    WHERE id = NEW.id;

    IF NEW.cancel_time <= delivery_request_submission_time THEN
        RAISE EXCEPTION 'The cancel time of a cancelled request should be after the submission time of the corresponding  
            delivery request.'
        RETURN NULL;
    ENDIF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-------------------------------------------------------------Return_legs related---------------------------------------------------------------------

/*For each delivery request, the first return_leg’s ID should 1, the second return_leg’s ID should be 2, and so on.*/

-------------------------------------------------------------------------------------------------------------------------------------

/*For a delivery request, the first return_leg cannot be inserted if (i) there is no existing leg for the delivery 
request or (ii) the last existing leg’s end_time is after the start_time of the return_leg. In addition, the 
return_leg’s start_time should be after the cancel_time of the request (if any). */

-------------------------------------------------------------------------------------------------------------------------------------

/*For each delivery request, there can be at most three unsuccessful_return_deliveries*/

---------------------------------------------------------Unsuccessful_return_deliveries related--------------------------------------------------------

/*The timestamp of each unsuccessful_return_delivery should be after the start_time of the 
corresponding return_leg. */

CREATE TRIGGER check_unsuccessful_return_delivery_timestamp_trigger
BEFORE INSERT ON unsuccessful_return_deliveries
FOR EACH ROW
EXECUTE FUNCTION check_unsuccessful_return_delivery_timestamp_func();

CREATE OR REPLACE FUNCTION check_unsuccessful_return_delivery_timestamp_func()
AS $$
DECLARE corr_return_leg_start_time TIMESTAMP
BEGIN
    SELECT start_time FROM legs INTO corr_return_leg_start_time
    WHERE leg_id = NEW.leg_id;

    IF NEW.attempt_time <= corr_return_leg_start_time THEN
        RAISE EXCEPTION 'The timestamp of each unsuccessful_return_delivery should be after the start_time of the 
            corresponding return_leg.'
        RETURN NULL;
    ENDIF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


