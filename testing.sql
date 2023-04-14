--- Intialization of procedure for submit_request
CREATE OR REPLACE PROCEDURE submit_request(
  customer_id INTEGER,
  evaluator_id INTEGER,
  pickup_addr TEXT,
  pickup_postal TEXT,
  recipient_name TEXT,
  recipient_addr TEXT,
  recipient_postal TEXT,
  submission_time TIMESTAMP,
  package_num INTEGER,
  reported_height INTEGER [],
  reported_width INTEGER [],
  reported_depth INTEGER [],
  reported_weight INTEGER [],
  content TEXT [],
  estimated_value NUMERIC []
) AS $$ 
DECLARE 
request_id INTEGER;
temp_val INTEGER;

BEGIN
INSERT INTO
  delivery_requests (
    customer_id,
    evaluater_id,
    status,
    pickup_addr,
    pickup_postal,
    recipient_name,
    recipient_addr,
    recipient_postal,
    submission_time,
    pickup_date,
    num_days_needed,
    price
  )
VALUES
  (
    customer_id,
    evaluator_id,
    'submitted',
    pickup_addr,
    pickup_postal,
    recipient_name,
    recipient_addr,
    recipient_postal,
    submission_time,
    NULL,
    NULL,
    NULL
  ) 
  RETURNING id INTO request_id;

temp_val := 1;

LOOP 
EXIT WHEN temp_val > package_num;

INSERT INTO
  packages (
    request_id,
    package_id,
    reported_height,
    reported_width,
    reported_depth,
    reported_weight,
    content,
    estimated_value,
    actual_height,
    actual_width,
    actual_depth,
    actual_weight
  )
VALUES
  (
    request_id,
    temp_val,
    reported_height [temp_val],
    reported_width [temp_val],
    reported_depth [temp_val],
    reported_weight [temp_val],
    content [temp_val],
    estimated_value [temp_val],
    NULL,
    NULL,
    NULL,
    NULL
  );

temp_val := temp_val + 1;

END LOOP;
END;

$$ LANGUAGE plpgsql;

--- Insertion of testing data
INSERT INTO Customers (name, gender, mobile) VALUES ('alice', 'female', '98765432');
INSERT INTO Customers (name, gender, mobile) VALUES ('bob', 'male', '98765431');
INSERT INTO Customers (name, gender, mobile) VALUES ('chris', 'female', '98765433');
INSERT INTO Customers (name, gender, mobile) VALUES ('dylan', 'male', '98765434');

INSERT INTO Employees (name, gender, dob, title, salary) VALUES ('employee1', 'female', '01-01-2000', 'evaluator', '3000');
INSERT INTO Employees (name, gender, dob, title, salary) VALUES ('employee2', 'female', '01-01-2000', 'delivery man', '3000');
INSERT INTO Employees (name, gender, dob, title, salary) VALUES ('employee3', 'male', '01-01-2000', 'manager', '3000');
INSERT INTO Employees (name, gender, dob, title, salary) VALUES ('employee4', 'male', '01-01-2000', 'delivery man', '3000');
INSERT INTO Employees (name, gender, dob, title, salary) VALUES ('employee5', 'male', '01-01-2000', 'nice guy', '3000');

INSERT INTO Delivery_Staff VALUES (2);
INSERT INTO Delivery_Staff VALUES (4);
INSERT INTO Delivery_Staff VALUES (5);

INSERT INTO Facilities (address, postal) VALUES ('facility address 1', '123123');
INSERT INTO Facilities (address, postal) VALUES ('facility address 2', '123133');
INSERT INTO Facilities (address, postal) VALUES ('facility address 3', '123143');
INSERT INTO Facilities (address, postal) VALUES ('facility address 4', '123153');
INSERT INTO Facilities (address, postal) VALUES ('facility address 5', '123163');

--- Insert procedure for request
CALL submit_request('1', '1', 'pickup_address', '123456', 'recipient_name', 'recipient_addr',
  '654321', '2016-06-22 19:10:25-07', '2', ARRAY[2, 2], ARRAY[2, 2], ARRAY[2, 2],
  ARRAY[2, 2], ARRAY['Content 1', 'Content 2'], ARRAY[2, 2]);

--- [POSITIVE] Test that a consecutive package_id for the same delivery_request works
INSERT INTO packages VALUES (3, 3, 12.5, 6.5, 10.2, 3.8, 'Pants', 50.0, 12.5, 6.5, 10.2, 3.8);

--- [NEGATIVE] Test that a non-consecutive package_id for the same delivery_request raises exception
INSERT INTO packages VALUES (3, 5, 12.5, 6.5, 10.2, 3.8, 'Pants', 50.0, 12.5, 6.5, 10.2, 3.8);

--- [NEGATIVE] Test that a delivery_request with missing package raises exception
INSERT INTO delivery_requests VALUES (1, 3, 1, 'completed', '123 Main St', '12345', 'John Doe', '456 Elm St', '67890', '2023-04-08 03:00:00', '2023-04-12', 3, 50.00);

--- [POSITIVE] Test that a consecutive ID for an unsuccessful pickup works
INSERT INTO accepted_requests VALUES(3, '333', '2022-04-11 03:00:00', 4);
INSERT INTO unsuccessful_pickups VALUES(3, 1, 5, '2022-04-11 03:00:00', 'fail');

--- [NEGATIVE] Test that a non-consecutive ID for an unsuccessful pickup raises exception
INSERT INTO unsuccessful_pickups VALUES(3, 3, 5, '2022-04-11 03:00:00', 'fail');


--- [POSITIVE] Test that the timestamp of the first unsuccessful pickup is after the submission_time and it works
delete from unsuccessful_pickups where request_id = 3;
insert into unsuccessful_pickups values(3, 1, 5, '2016-06-22 19:10:25.001', 'fail');

--- [POSITIVE] Test that the timestamp of the first unssuccessful pickup is after the submission_time and subsequent pickup time after the prev works
delete from unsuccessful_pickups where request_id = 3;
insert into unsuccessful_pickups values(3, 1, 5, '2016-06-22 19:10:25.001', 'fail');
insert into unsuccessful_pickups values(3, 2, 5, '2016-06-22 19:10:25.002', 'fail');

--- [NEGATIVE] Test that the timestamp of the first unsuccessful pickup is before the submission_time and raises exception
delete from unsuccessful_pickups where request_id = 3;
insert into unsuccessful_pickups values(3, 1, 5, '2016-06-22 19:10:24.000', 'fail');

--- [NEGATIVE] Test that the timestamp of the first unsuccessful pickup is after the submission_time and subsequent pickup time are before the prev and raises an exception
delete from unsuccessful_pickups where request_id = 3;
insert into unsuccessful_pickups values(3, 1, 5, '2016-06-22 19:10:25.007', 'fail');
insert into unsuccessful_pickups values(3, 2, 5, '2016-06-22 19:10:24.003', 'fail');


--- [POSITIVE] Test that a consecutive id for legs for the same delivery_request works
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)

--- [NEGATIVE] Test that a non-consecutive id for legs for the same delivery_request raises an exception
INSERT INTO legs VALUES(3, 2, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)

--- [POSITIVE] Test that the timestamp of the first leg is after the submission_time and it works
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)

--- [POSITIVE] Test that the timestamp of the first leg is after the submission_time and last_unsuccessful_pick after the prev works
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)

--- [NEGATIVE] Test that the timestamp of the first leg is before the submission_time and raises exception
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2016-06-22 19:10:24.000', '2023-04-18 10:35:00', 1); --(CUST,1)

--- [NEGATIVE] Test that the timestamp of the first unsuccessful pickup is after the submission_time and start_time is before last_unsuccessful_pickup and raise an exception
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2016-06-22 19:10:25.005', '2023-04-18 10:35:00', 1); --(CUST,1)


--- Test for trigger 7
--- [POSITIVE]
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1);
INSERT INTO legs VALUES(3, 2, 2, '2023-04-18 10:40:00', '2023-04-18 10:35:00', 1);

--- [NEGAIVE] Start time of each leg is before end time of previous leg.
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1);
INSERT INTO legs VALUES(3, 2, 2, '2022-04-18 10:00:00', '2022-04-18 10:35:00', 1);

--- [NEGAIVE] End time of previous leg NULL.
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', NULL, 1);
INSERT INTO legs VALUES(3, 2, 2, '2022-04-18 10:00:00', '2022-04-18 10:35:00', 1);





--- [POSITIVE] Time stamp of each unsuccessful_delivery should be after start_time of corresponding leg
INSERT INTO legs VALUES(3, 2, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)
INSERT INTO unsuccessful_deliveries VALUES (3, 1, 'Fell asleep', '2023-04-18 11:00:00');
INSERT INTO unsuccessful_deliveries VALUES (3, 2, 'Fell asleep', '2023-04-18 12:00:00');

--- [NEGATIVE] Timestamp fo each unsuccessful_delivery is before and raises an exception
delete from unsuccessful_deliveries where request_id = 3;
INSERT INTO unsuccessful_deliveries VALUES (3, 1, 'Fell asleep', '2016-06-22 19:09:24.000');
INSERT INTO unsuccessful_deliveries VALUES (3, 2, 'Fell asleep', '2016-06-22 19:09:24.000');


--- [NEGATIVE] More than three unsuccessful_delivery raises an exception
INSERT INTO legs VALUES(3, 3, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)
INSERT INTO legs VALUES(3, 4, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1); --(CUST,1)
delete from unsuccessful_deliveries where request_id = 3;
INSERT INTO unsuccessful_deliveries VALUES (3, 1, 'Fell asleep', '2023-04-18 11:00:00');
INSERT INTO unsuccessful_deliveries VALUES (3, 2, 'Fell asleep', '2023-04-18 11:00:00');
INSERT INTO unsuccessful_deliveries VALUES (3, 3, 'Fell asleep', '2023-04-18 11:00:00');
--- This one im a bit unsure, can there be a leg with id 4?
INSERT INTO unsuccessful_deliveries VALUES (3, 4, 'Fell asleep', '2023-04-18 11:00:00');



--- [POSITIVE] cancel_time after submission_time and works
insert into cancelled_requests values (3, '2023-04-18 10:00:00');

--- [NEGATIVE] cancel_time after submission_time and raises an exception
delete from cancelled_requests where id = 3;
insert into cancelled_requests values (3, '2016-06-22 19:10:24.000');


--- [POSITIVE] For each delivery request, first = 1, second = 2, ...
INSERT INTO cancelled_or_unsuccessful_requests VALUES (3);
INSERT INTO return_legs VALUES(3, 1, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 2, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 3, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');


--- [NEGATIVE] first = 1, second = 3
delete from return_legs where request_id  = 3;
INSERT INTO return_legs VALUES(3, 1, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 3, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');



--- Trigger 12
--- [POSITVE]
delete from unsuccessful_return_deliveries where request_id = 3;
delete from return_legs where request_id  = 3;
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2023-04-18 10:00:00', '2023-04-18 10:35:00', 1);
INSERT INTO return_legs VALUES(3, 1, 2, '2024-04-24 10:05:06', 1, '2024-04-24 10:35:06');

--- [NEGATIVE] For a delivery request, the first return_leg cannot be inserted if there is no existing leg for the delivery 
--- request
delete from return_legs where request_id  = 3;
delete from legs where request_id = 3;
INSERT INTO return_legs VALUES(3, 1, 2, '2023-04-24 10:05:06', 1, '2023-04-24 10:35:06');

--- [NEGATIVE] For a delivery request, the first return_leg cannot be inserted if, the last existing leg’s end_time is after the start_time
--- of the return_leg. In addition, the return_leg’s start_time should be after the cancel_time of the request (if any)
delete from legs where request_id = 3;
delete from return_legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2022-04-18 10:00:00', '2022-04-18 10:35:00', 1);
INSERT INTO return_legs VALUES(3, 1, 2, '2021-04-10 10:05:06', 1, '2021-04-24 10:35:06');

--- Trigger 13
--- [NEGATIVE] More than three unsuccessful_return_deliveries
delete from unsuccessful_return_deliveries where request_id = 3;
delete from return_legs where request_id = 3;
delete from legs where request_id = 3;
INSERT INTO legs VALUES(3, 1, 2, '2022-04-18 10:00:00', '2022-04-18 10:35:00', 1);
INSERT INTO legs VALUES(3, 2, 2, '2022-04-18 10:40:00', '2022-04-18 10:35:00', 1);
INSERT INTO legs VALUES(3, 3, 2, '2022-04-18 10:41:00', '2022-04-18 10:35:00', 1);
INSERT INTO legs VALUES(3, 4, 2, '2022-04-18 10:42:00', '2022-04-18 10:35:00', 1);
INSERT INTO return_legs VALUES(3, 1, 2, '2023-04-10 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 2, 2, '2023-04-10 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 3, 2, '2023-04-10 10:05:06', 1, '2023-04-24 10:35:06');
INSERT INTO return_legs VALUES(3, 4, 2, '2023-04-10 10:05:06', 1, '2023-04-24 10:35:06');
insert into unsuccessful_return_deliveries values (3, 1, 'toh', '2023-04-24 10:05:10.000');
insert into unsuccessful_return_deliveries values (3, 2, 'toh', '2023-04-24 10:05:10.000');
insert into unsuccessful_return_deliveries values (3, 3, 'toh', '2023-04-24 10:05:10.000');
insert into unsuccessful_return_deliveries values (3, 4, 'toh', '2023-04-24 10:05:10.000');



--- [POSITIVE] Timestamp of each unsuccessful_return_delivery should be after the start_time of the corresponding return_leg and works
insert into unsuccessful_return_deliveries values (3, 1, 'toh', '2023-04-24 10:05:10.000');

--- [NEGATIVE] Timestamp of each unsuccessful_return_delivery before the start_time of the corresponding return_leg and raises exception
delete from unsuccessful_deliveries where request_id = 3;
insert into unsuccessful_return_deliveries values (3, 1, 'toh', '2023-04-24 10:00:00.000');
