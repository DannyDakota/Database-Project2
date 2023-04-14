--------------------------------------------------------Delivery_requests related-----------------------------------------------------

-- (1) Each delivery request has at least one package

CREATE OR REPLACE FUNCTION check_packages_func() RETURNS TRIGGER
AS $$
begin
  IF NOT EXISTS (SELECT 1 FROM packages p WHERE p.request_id = NEW.id) then
    RAISE EXCEPTION 'Each delivery request must have at least one package';
    RETURN NULL;
  else
    RETURN NEW;
  END IF; 
end;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER req_has_package
after INSERT ON delivery_requests
deferrable initially deferred
FOR EACH ROW EXECUTE FUNCTION check_packages_func();

CREATE OR REPLACE FUNCTION delivery_request_one_package_dr_func() 
RETURNS TRIGGER AS $$ 
BEGIN 
IF NOT EXISTS (
  SELECT
    1
  FROM
    packages
  WHERE
    (request_id = NEW.id)
) 
THEN RAISE EXCEPTION 'Each delivery request needs to have at least one package!';

END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER delivery_request_one_package_dr_check
AFTER INSERT ON delivery_requests 
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW EXECUTE FUNCTION delivery_request_one_package_dr_func();

--------------------------------------------------------Package related---------------------------------------------------------------

-- (2) For each delivery request, the IDs of the packages should be consecutive integers starting from 1.


CREATE OR REPLACE FUNCTION check_consecutive_func() RETURNS TRIGGER 
AS $$
DECLARE max_id INT;
BEGIN
    SELECT MAX(package_id) FROM packages into max_id where packages.request_id = new.request_id;
    max_id := max_id + 1;
    IF (NEW.package_id = max_id) THEN
        RETURN NEW;
    ELSEIF (max_id is NULL) THEN
        NEW.package_id := 1;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'The IDs of the packages should be consecutive integers';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_consecutive
BEFORE INSERT ON packages
FOR EACH ROW EXECUTE FUNCTION check_consecutive_func();


------------------------------------------------------Unsuccessful_pickups related------------------------------------------------

-- (3) For each delivery request, the IDs of the unsuccessful pickups should be consecutive integers starting from 1.

CREATE OR REPLACE FUNCTION check_unsucc_pickup_func()
returns  trigger AS $$
DECLARE max_unsucc_pickup_id NUMERIC;
begin
    SELECT MAX(pickup_id) INTO max_unsucc_pickup_id
    FROM unsuccessful_pickups u
    WHERE u.request_id =  NEW.request_id;
    IF max_unsucc_pickup_id IS NOT NULL AND (max_unsucc_pickup_id != COALESCE(NEW.pickup_id,0) - 1) THEN
        RAISE EXCEPTION 'the IDs of the unsuccessful pickups should be consecutive integers starting from 1';
        RETURN NULL;
    END IF;
    IF max_unsucc_pickup_id IS NULL AND NEW.pickup_id IS NULL then
        NEW.pickup_id := 1;
    END IF;
    RETURN NEW;
end;
$$ LANGUAGE plpgsql;

CREATE or replace TRIGGER check_pickup_trigger
BEFORE INSERT ON unsuccessful_pickups
FOR EACH ROW EXECUTE FUNCTION check_unsucc_pickup_func();

-----------------------------------------------------------------------------------------------------------------------------------

/* (4) The timestamp of the first unsuccessful pickup should be after the submission_time of the 
corresponding delivery request. In addition, each unsuccessful pickup’s timestamp should be after the 
previous unsuccessful pickup’s timestamp (if any).*/

CREATE OR REPLACE FUNCTION check_pickup_timestamp_func()
returns trigger AS $$
DECLARE delivery_request_timestamp TIMESTAMP;
DECLARE first_unsucc_pickup_timestamp TIMESTAMP;
DECLARE last_unsucc_pickup_timestamp TIMESTAMP;
begin
    SELECT d.submission_time into delivery_request_timestamp
    FROM delivery_requests d
    WHERE NEW.request_id = d.id;

    SELECT pickup_time INTO last_unsucc_pickup_timestamp
    FROM unsuccessful_pickups u 
    WHERE u.request_id = NEW.request_id and u.pickup_id = new.pickup_id - 1;

   	if (new.pickup_id = 1) then
		if (new.pickup_time < delivery_request_timestamp) then
		RAISE EXCEPTION 'The timestamp of the first unsuccessful pickup should be after the submission_time of the 
	            corresponding delivery request';
	        return NULL;
	    end if;
   	end if;
    IF (NEW.pickup_time < last_unsucc_pickup_timestamp) then
    RAISE EXCEPTION 'Each unsuccessful pickup’s timestamp should be after the previous unsuccessful pickup’s timestamp';
        return NULL;
    END IF;
    return NEW;
end;
$$ LANGUAGE plpgsql;


------------------------------------------------------------Legs related--------------------------------------------------------------

-- (5) For each delivery request, the IDs of the legs should be consecutive integers starting from 1. 


CREATE OR REPLACE FUNCTION check_legs_ID_func() returns trigger
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

CREATE or replace TRIGGER check_legs_ID_trigger
BEFORE INSERT ON legs
FOR EACH ROW EXECUTE FUNCTION check_legs_ID_func();

-------------------------------------------------------------------------------------------------------------------------------------

/* (6) For  each  delivery  request,  the  start  time  of  the  first  leg  should  be  after  the  submission_time  of  the  
delivery request and the timestamp of the last unsuccessful pickup (if any). */

CREATE OR REPLACE FUNCTION check_first_leg_start_time_func()
returns trigger AS $$

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

CREATE or replace TRIGGER check_first_leg_start_time_trigger
BEFORE INSERT ON legs
WHEN (NEW.leg_id = 1)
FOR EACH ROW EXECUTE FUNCTION check_first_leg_start_time_func();

---------------------------------------------------------------------------------------------------------------------------------------------

/* (7) For each delivery request, a new leg cannot be inserted if its start_time is before the end_time of the 
previous leg, or if the end_time of the previous leg is NULL.*/


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
    ELSE NEW.start_time < previous_leg_end_time THEN
        RAISE EXCEPTION 'A new leg cannot be inserted if it is before the end time of previous leg.';
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_legs_start_time_trigger
BEFORE INSERT ON legs
FOR EACH ROW 
WHEN (NEW.leg_id > 1)
EXECUTE FUNCTION check_legs_start_time_func();

---------------------------------------------------------------Unsuccessful_deliveries----------------------------------------------------------------------

/* (8) The timestamp of each unsuccessful_delivery should be after the start_time of the corresponding leg.*/


CREATE OR REPLACE FUNCTION check_unsuccessful_delivery_timestamp_trigger_func()
returns trigger AS $$
DECLARE leg_start_time TIMESTAMP;
BEGIN
    SELECT start_time FROM legs
    INTO leg_start_time
    WHERE leg_id = NEW.leg_id;

    IF NEW.attempt_time <= leg_start_time THEN
        RAISE EXCEPTION 'The timestamp of each unsuccessful delivery should be after the start time of the corresponding leg.';
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_unsuccessful_delivery_timestamp_trigger
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW 
EXECUTE FUNCTION check_unsuccessful_delivery_timestamp_trigger_func();

-------------------------------------------------------------------------------------------------------------------------------------
/* (9) For each delivery request, there can be at most three unsuccessful_deliveries.*/


CREATE OR REPLACE FUNCTION check_num_of_unsuccessful_deliveries_func()
returns trigger AS $$
DECLARE curr_num_of_unsuccessful_deliveries INT;
BEGIN
    SELECT COUNT(*) FROM unsuccessful_deliveries INTO curr_num_of_unsuccessful_deliveries
    WHERE request_id = NEW.request_id;

    IF curr_num_of_unsuccessful_deliveries >= 3 THEN
        RAISE EXCEPTION 'For each delivery request, there can be at most three unsuccessful_deliveries.';
        RETURN NULL;
    END if;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_num_of_unsuccessful_deliveries_trigger
BEFORE INSERT ON unsuccessful_deliveries
FOR EACH ROW
EXECUTE FUNCTION check_num_of_unsuccessful_deliveries_func();

------------------------------------------------------------Cancelled_requests related-----------------------------------------------

/* (10) The cancel time of a cancelled request should be after the submission time of the corresponding delivery request.*/


CREATE OR REPLACE FUNCTION check_cancel_time_of_cancelled_request_func()
returns trigger AS $$
DECLARE delivery_request_submission_time TIMESTAMP;
BEGIN
    SELECT submission_time FROM delivery_requests INTO delivery_request_submission_time
    WHERE id = NEW.id;

    IF NEW.cancel_time <= delivery_request_submission_time THEN
        RAISE EXCEPTION 'The cancel time of a cancelled request should be after the submission time of the corresponding  
            delivery request.';
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_cancel_time_of_cancelled_request_trigger
BEFORE INSERT ON cancelled_requests
FOR EACH ROW
EXECUTE FUNCTION check_cancel_time_of_cancelled_request_func();

-------------------------------------------------------------Return_legs related---------------------------------------------------------------------

/* (11) For each delivery request, the first return_leg’s ID should 1, the second return_leg’s ID should be 2, and so on.*/


CREATE OR REPLACE FUNCTION check_return_leg_id_func()
returns trigger AS $$
DECLARE current_leg_id_count NUMERIC;
BEGIN
    SELECT MAX(leg_id) INTO current_leg_id_count
    FROM return_legs l
    WHERE l.request_id = NEW.request_id;
    IF current_leg_id_count IS NULL AND NEW.leg_id = 1 THEN
        RETURN NEW;
    ELSIF current_leg_id_count IS NOT NULL AND (NEW.LEG_ID = current_leg_id_count + 1)then
        RETURN NEW;
    else
    	RAISE EXCEPTION 'Return_leg id should be consecutive.';
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE or replace TRIGGER check_return_leg_id_trigger
BEFORE INSERT ON return_legs
FOR EACH ROW EXECUTE FUNCTION check_return_leg_id_func();
-------------------------------------------------------------------------------------------------------------------------------------

/* (12) For a delivery request, the first return_leg cannot be inserted if (i) there is no existing leg for the delivery 
request or (ii) the last existing leg’s end_time is after the start_time of the return_leg. In addition, the 
return_leg’s start_time should be after the cancel_time of the request (if any). */


CREATE OR REPLACE FUNCTION check_return_leg_insertion_func();
AS $$
DECLARE last_leg_timestamp TIMESTAMP;
DECLARE cancel_time_timestamp TIMESTAMP;
BEGIN
    -- TODO: Check if this applies to the second return_leg as well
    IF NOT EXISTS (SELECT * FROM legs l WHERE NEW.request_id = l.request_id) THEN
        RAISE EXCEPTION "The first return_leg cannot be inserted if (i) there is no existing leg for the delivery 
request";
        RETURN NULL;
    END IF;
    -- TODO: Check if MAX(end_time) is correct, note that endtime can be null
    SELECT COALESCE(MAX(end_time),'1900-01-01 00:00:00')  INTO last_leg_timestamp
    FROM legs l
    WHERE l.request_id = NEW.request_id;
    IF last_leg_timestamp > NEW.start_time THEN 
        RAISE EXCEPTION "Last existing leg’s end_time should not be after the start_time of the return_leg";
        RETURN NULL;
    END IF;
    
    SELECT COALESCE(cancel_time, '1900-01-01 00:00:00') INTO cancel_time_timestamp
    FROM cancelled_requests cr
    WHERE cr.id = NEW.request_id;

    IF cancel_time_timestamp AND cancel_time_timestamp > NEW.start_time THEN
        RAISE EXCEPTION "Return leg's start time should be after cancel_time";
        RETURN NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_return_leg_insertion_trigger
BEFORE INSERT ON return_legs
FOR EACH ROW EXECUTE FUNCTION check_return_leg_insertion_func();

-------------------------------------------------------------------------------------------------------------------------------------

/* (13) For each delivery request, there can be at most three unsuccessful_return_deliveries*/

CREATE OR REPLACE FUNCTION check_no_of_unsucc_return_deliv_func()
AS $$
DECLARE no_of_unsuccess NUMERIC;
BEGIN
    SELECT COUNT(*) INTO no_of_unsuccess
    FROM unsuccessful_return_deliveries u
    WHERE new.request_id = u.request_id;

    IF no_of_unsuccess >= 3 THEN
        RAISE EXCEPTION 'Can only be at most three unsuccessful_return_deliveries';
        RETURN NULL;
    END IF;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_no_of_unsucc_return_deliv_trigger
BEFORE INSERT ON unsuccessful_return_deliveries
FOR EACH ROW EXECUTE check_no_of_unsucc_return_deliv_func();

---------------------------------------------------------Unsuccessful_return_deliveries related--------------------------------------------------------

/* (14) The timestamp of each unsuccessful_return_delivery should be after the start_time of the 
corresponding return_leg. */

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
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_unsuccessful_return_delivery_timestamp_trigger
BEFORE INSERT ON unsuccessful_return_deliveries
FOR EACH ROW
EXECUTE FUNCTION check_unsuccessful_return_delivery_timestamp_func();
