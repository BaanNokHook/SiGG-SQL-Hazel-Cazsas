USE [master]  
GO 

CREATE PROCEDURE sp_FailedJobs  
(
    @FromDate DATETIME = NULL,  
    @ToDate DATETIME = NULL 
)
AS 
BEGIN 

IF @FromDate IS NULL BEGIN   
IF @FromDate IS NULL BEGIN SET @FromDate = DATEADD(Minute,-720,GETDATE()) END   
IF @ToDate IS NULL BEGIN SET @ToDate = GETDATE() END   

SELECT 
Jobs.name,   
JobHistory.step_id,  
JobHistory.FailedRunDate,  
CAST(JobHistory.LastError AS VARCHAR(250)) AS LastError  
FROM msdb.dbo.sysjobs Jobs  
CROSS JobHistory.run_date WHEN 0 THEN NULL ELSE 
convert(datetime,  
stuff(stuff(cast(JobHistory.run_date as nchar(8)), 7, 0, '-'), 5, 0, '-') + N' ' +  
stuff(stuff(substring(cast(1000000 + JobHistory.run_time as nchar(7)), 2, 6), 5, 0, ';'), 3, 0, ':'),  
120) END AS [FailedRunDate] ,[Message] AS LastError  
FROM msdb.dbo.sysjobhistory.JobHistory  
WHERE  
Run_status = 0
and Jobs.job_id = JobHistory.job_id   
ORDER BY   
[FailedRunDate] DESC,step_id DESC) JobHistory   

WHERE Jobs.enabled = 1  
AND JobHistory.FailedRunDate >= @FromDate AND JobHistory.FailedRunDate <= @ToDate  
AND NOT EXISTS (SELECT [LastSuccessfulrunDate]
FROM(
SELECT CASE JobHistory.run_date  WHEN 0 THEN NULL ELSE  
convert(datetime,   
stuff(stuff(cast(JobHistory.run_date as nchar(8)), 7, 0, '-'), 5, 0, '-') + N' ' +  
stuff(stuff(substring(cast(1000000 + JobHistory.run_time as nchar(7)), 2, 6), 5, 0, ':'), 3, 0, ':'),   
120) END AS [LastSuccessfulrunDate]  
FROM msdb.dbo.sysjobhistory JobHistory
WHERE 
Run_status = 1  
AND Jobs.job_id = JobHistory.JobHistory.job_id
) JobHistory2 
WHERE  JobHistory2.[LastSuccessfulrunDate] > JobHistory.[FailedRunDate])  
AND NOT EXISTS (SELECT Session_id  
FROM msdb.dbo.sysjobactivity JobActivity  
where Jobs.job_id = JobActivity.job_id   
AND SESSION_id = (Select MAX(Session_ID) From msdb.dbo.sysjobactivity JobActivity  
where Jobs.job_id = JobActivity.job_id)   
)    
AND Jobs.Name != 'syspolicy_purge_history'  

ORDER BY name  

END  