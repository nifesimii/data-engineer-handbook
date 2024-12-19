--Creating a monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE hosts_activity_reduced(
     month DATE, -- the month column 
     host text,     -- the host column
     hit_metric TEXT, -- the metric for hits
     hit_array REAL[], -- the array for the month for hits 
     unique_visitors_metric text, --the metric for unique visitor hits
     unique_visitors_array REAL[],--the array for the  metric for unique visitor hits
     primary KEY(month,host,hit_metric,unique_visitors_metric) -- the primary key based on the columns month,host,hit_metric,unique_visitors_metric
);