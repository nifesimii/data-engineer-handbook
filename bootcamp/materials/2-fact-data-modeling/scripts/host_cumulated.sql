--Creating A DDL for hosts_cumulated table containing the host_activity_datelist which logs to see which dates each host is experiencing any activity
CREATE TABLE hosts_cumulated(
     host text, --the host website 
     device_activity date[], --the datelist column 
     date date, -- the date column 
     primary KEY(host,date) -- the primary key based on the host and date column
);