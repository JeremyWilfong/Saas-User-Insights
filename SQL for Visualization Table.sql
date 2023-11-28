--CREATING A LARGE TABLE FOR VISUALIZATION OF COMMON USER ACTIVITY. 

--most common activitytype per user
select u.userID, count(activityType) as type_count, activityType
into #activityType_count
from activity_data a
join user_data u on a.userID=u.userID
group by u.userID, activityType
order by u.userID

select userID, activityType, type_count, 
	row_number() over(partition by userID order by type_count desc) as rank
into #ranked_activityType
from #activityType_count

select userID, activityType
into #most_popular_activityType
from #ranked_activityType
where rank = 1
order by activityType 

--most popular device type per user
select u.userID, count(deviceType) as deviceType_count, deviceType
into #deviceType_count
from login_history_data l
join user_data u on l.userID=u.userID
group by u.userID, deviceType
order by u.userID

select userID, deviceType, deviceType_count, 
	row_number() over(partition by userID order by deviceType_count desc) as rank
into #ranked_deviceType
from #deviceType_count

select userID, deviceType
into #most_popular_deviceType
from #ranked_deviceType
where rank = 1
order by deviceType 

--most popular account per user (excluding checkings and savings)
WITH AccountTypeCounts AS (
    select a.userID, a.accountType, 
        COUNT(*) AS accountType_count
    from account_data a
    where a.accountType NOT IN ('Checking', 'Savings')
    group by a.userID, a.accountType
), RankedAccountTypes AS (
    select c.userID, c.accountType, c.accountType_count, 
        ROW_NUMBER() OVER (PARTITION BY c.userID ORDER BY c.accountType_count DESC) AS rank
    from AccountTypeCounts c
), AllUsers AS (
    select distinct u.userID
    from user_data u
), MostPopularAccountType AS (
    select r.userID, r.accountType
    from RankedAccountTypes r
    where r.rank = 1
)
select  u.userID, ISNULL(m.accountType, 'Only Checking and Savings') AS MostPopularAccountType
into #most_popular_accountType
from AllUsers u
left join MostPopularAccountType m ON u.userID = m.userID
order by u.userID;


--average number of activities per login per user
select userID, loginID, count(activityID) as activity_count
into #activity_count_per_login_per_user
from activity_data
group by userID, loginID
order by userID

select userID, AVG(activity_count) as avg_#_of_activities_per_login
into #avg_#_of_activities_per_login
from #activity_count_per_login_per_user
group by userID
order by userID

--Number of logins per user
select userID, count(loginID) as #_of_logins 
into #times_logged_in 
from login_history_data 
group by userID 
order by userID

--Most Recent Login
select l.userID, max(loginTime) as last_logged_in
into #last_login_date
from login_history_data l
group by l.userID

--Last Activity Type completed.
WITH RecentActivity AS (
    select 
        act.userID, 
        act.activityTime,
        act.activityType,
        ROW_NUMBER() OVER (PARTITION BY act.userID ORDER BY act.activityTime DESC) AS rn
    from activity_data act
    inner join #last_login_date ll ON act.userID = ll.userID AND act.activityTime > ll.last_logged_in
)
select 
    userID, 
    activityTime AS last_activity_time, 
    activityType
into #last_activity_after_login
from RecentActivity
where rn = 1;

--Last Device Used
WITH RecentDevice AS (
    select 
        l.userID, 
        l.deviceType,
        ROW_NUMBER() OVER (PARTITION BY l.userID ORDER BY l.loginTime DESC) AS rn
    from login_history_data l
    inner join #last_login_date ll ON l.userID = ll.userID
)
select 
    userID, 
    deviceType
into #last_device_used
from RecentDevice
where rn = 1;

--Most common transaction category
select u.userID, count(category) as transactionCategory_count, category
into #transactionCategory_count
from transactions_data t
join account_data a on t.accountID=a.accountID
join user_data u on a.userID=u.userID
group by u.userID, category
order by u.userID


select userID, category, transactionCategory_count, 
	row_number() over(partition by userID order by transactionCategory_count desc) as rank
into #ranked_transactionCategory
from #transactionCategory_count

select userID, category
into #most_popular_transactionCategory
from #ranked_transactionCategory
where rank = 1
order by category 

--avg income per user.
select u.userID,
	AVG(CASE WHEN t.category = 'Income' THEN t.amount ELSE NULL END) AS avg_income
into #avg_income
from user_data u
left join account_data a on u.userID=a.userID
left join transactions_data t ON a.accountID = t.accountID AND t.category = 'Income'
group by u.userID

--Average days between each login
WITH OrderedLogins AS (
    select 
        loginID, 
        userID, 
        loginTime,
        LAG(loginTime) OVER (PARTITION BY userID ORDER BY loginTime) AS previousLoginTime
    from login_history_data
),
LoginDifferences AS (
    select 
        userID, 
        DATEDIFF(day, previousLoginTime, loginTime) AS daysBetweenLogins
    from OrderedLogins
    where previousLoginTime IS NOT NULL
)
select 
    u.userID, 
    AVG(daysBetweenLogins) AS avgDaysBetweenLogins
into #avg_days_between_login
from user_data u
left join LoginDifferences ld on u.userID=ld.userID
group by u.userID


--TABLE aggregating all the temp tables above for visualization in Tableau.
select u.userID, gender, DATEDIFF(year,dateOfBirth, CURRENT_TIMESTAMP) as age, city, state,membershipType, CreditScore, signUpDate,
tli.#_of_logins as #_times_logged_in,  
avg(datediff(MINUTE, loginTime, logoutTime)) as avg_mins_logged_in,
mpa.activityType as most_common_activityType,
mpacc.MostPopularAccountType as most_common_accountType,
apl.avg_#_of_activities_per_login,
lld.last_logged_in,
DATEDIFF(day, last_logged_in, CURRENT_TIMESTAMP) as days_since_login,
la.activityType as most_recent_activity,
ldu.deviceType as most_recent_device_used,
mpt.category as most_common_transaction_type,
round(ai.avg_income,2) as avg_income_payment,
adl.avgDaysBetweenLogins as avg_days_between_login
from login_history_data l
join user_data u on l.userID=u.userID
join account_data a on u.userID=a.userID
join activity_data act on u.userID=act.userID
join #most_popular_activityType mpa on u.userID=mpa.userID
join #most_popular_deviceType mpd on u.userID=mpd.userID
join #most_popular_accountType mpacc on u.userID=mpacc.userID
join #avg_#_of_activities_per_login apl on u.userID=apl.userID
join #times_logged_in tli on u.userID=tli.userID
join #last_login_date lld on u.userID=lld.userID
join #last_activity_after_login la on u.userID=la.userID
join #last_device_used ldu on u.userID=ldu.userID
join #most_popular_transactionCategory mpt on u.userID=mpt.userID
join #avg_income ai on u.userID=ai.userID
join #avg_days_between_login adl on u.userID=adl.userID
group by u.userID, gender, dateOfBirth, city, state,membershipType, CreditScore, signUpDate, mpa.activityType, mpd.deviceType, mpacc.MostPopularAccountType, apl.avg_#_of_activities_per_login, tli.#_of_logins, lld.last_logged_in, la.activityType, ldu.deviceType, mpt.category, ai.avg_income, adl.avgDaysBetweenLogins
order by u.userID
