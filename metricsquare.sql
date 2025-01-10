# Creating database named metric_square
create database metric_square;

# Workin in metric_square database
use metric_square;

# Creating table named USERS
CREATE TABLE users (
    user_id INT,
    created_at VARCHAR(50),
    company_id INT,
    language VARCHAR(50),
    activated_at VARCHAR(100),
    state VARCHAR(50)
);

# Loading data in USERS table
show variables like 'secure_file_priv';

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

# Visualizing data in USERS
SELECT 
    *
FROM
    users;

# Changing datatype in column created_at from string to datetime
alter table users add column temp_created_at datetime;
UPDATE users 
SET 
    temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

# Changing datatype in column activated_at from string to datetime
alter table users add column temp_activated_at datetime;
UPDATE users 
SET 
    temp_activated_at = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');
alter table users drop column activated_at;
alter table users change column temp_activated_at activated_at datetime;

# Creating EVENTS table
CREATE TABLE events (
    user_id INT,
    occurred_at VARCHAR(100),
    event_type VARCHAR(50),
    event_name VARCHAR(50),
    location VARCHAR(20),
    device VARCHAR(50),
    user_type INT
);

# Loading data in table EVENTS
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

# Vizualizing data in EVENTS table
SELECT 
    *
FROM
    events;

# Changing datatype in column occurred_at from string to datetime
alter table events add column temp_occurred_at datetime;
UPDATE events 
SET 
    temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp_occurred_at occurred_at datetime;
    
# Creating a table named EMAIL_EVENTS
CREATE TABLE email_events (
    user_id INT,
    occured_at VARCHAR(100),
    action VARCHAR(100),
    user_type INT
);

# Loading data in table EVENTS
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

# Vizualizing data in EMAIL_EVENTS table
SELECT 
    *
FROM
    email_events;

# Changing datatype in column occurred_at from string to datetime
alter table email_events add column temp_occurred_at datetime;
UPDATE email_events 
SET 
    temp_occurred_at = STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i');
alter table email_events drop column occured_at;
alter table email_events change column temp_occurred_at occured_at datetime;

# A) Weekly User Engagement
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS no_of_weeks,
    COUNT(DISTINCT user_id) AS user_engagement
FROM
    events
WHERE
    event_type = 'Engagement'
GROUP BY no_of_weeks
ORDER BY no_of_weeks;

# B) User Growth Analysis
SELECT year, month, active_users,
		sum(active_users) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS user_growth,
    CASE 
        WHEN LAG(active_users) OVER (ORDER BY year, month) IS NULL THEN 0
        ELSE ROUND(((active_users - LAG(active_users) OVER (ORDER BY year, month)) / LAG(active_users) OVER (ORDER BY year, month)) * 100, 2)
    END AS growth_percentage
FROM (
	SELECT YEAR(activated_at) AS year,
			EXTRACT(MONTH FROM activated_at) AS month,
				COUNT(DISTINCT user_id) AS active_users
		FROM users
		GROUP BY year,month
		) as Active_Users_per_Month
ORDER BY year;
    
# C) Weekly Retention Analysis
SELECT 
    EXTRACT(WEEK FROM occurred_at) AS weeks,
    COUNT(DISTINCT user_id) AS no_of_users
FROM
    events
WHERE
    event_type = 'signup_flow'
        AND event_name = 'complete_signup'
GROUP BY weeks
ORDER BY weeks;
 

# D) Weekly Engagement Per Device
SELECT 
    EXTRACT(YEAR FROM occurred_at) AS year,
    EXTRACT(WEEK FROM occurred_at) AS week,
    device,
    COUNT(DISTINCT user_id) AS num_users
FROM 
    events
GROUP BY 
    year, week, device
ORDER BY 
    year, week, device;


# E) Email Engagement Analysis
SELECT 
    user_id,
    Emails_sent,
    Emails_opened,
    Emails_clicked,
    ROUND(SUM(Emails_opened) / SUM(Emails_sent), 2) * 100 AS Opening_rate,
    ROUND(SUM(Emails_clicked) / SUM(Emails_opened),
            2) * 100 AS Engagement_rate
FROM
    (SELECT 
        user_id,
            SUM(CASE
                WHEN `action` = 'sent_weekly_digest' THEN 1
                ELSE 0
            END) AS Emails_sent,
            SUM(CASE
                WHEN `action` = 'email_open' THEN 1
                ELSE 0
            END) AS Emails_opened,
            SUM(CASE
                WHEN `action` = 'email_clickthrough' THEN 1
                ELSE 0
            END) AS Emails_clicked
    FROM
        email_events
    GROUP BY user_id) AS user_email_engagement
GROUP BY user_id;