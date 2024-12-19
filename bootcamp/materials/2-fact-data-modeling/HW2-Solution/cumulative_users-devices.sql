--Creating ddl table for a cumulative user_devices table
--Creating the user_device cumulative table which includes the date list column 
CREATE TABLE user_devices_cumulated(
     user_id numeric, -- numeric data type is used 
     device_activity date[], -- the datelost column is used
     browser_type text, -- there's a seperate column for browser type
     date date, -- Date column from the vent table is used
     primary KEY(user_id,browser_type,date) -- primary key involving ser_id,browser type and date is used
);