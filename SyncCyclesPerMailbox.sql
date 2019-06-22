SELECT
          m.EmailAddress as EmailAddress,
          M.OwnerIdName as MailboxName,
          q.MailboxProcessStartedOn_Prev as 'MB_ProcessStartedOn'
       ,q.MailboxProcessStartedOn as 'MB_NextProcessStartedOn'
          ,DATEDIFF(MINUTE, q.MailboxProcessStartedOn_prev, q.MailboxProcessStartedOn) AS 'MinutesBetweenCycles'
       ,q.MachineName 'AsyncServer'
          ,q.OperationTypeId
       ,q.ProcessResult
       ,q.ItemsProcessed
/*
       ,q.MailboxProcessScheduledOn
       ,q.IndividualStepDurations
       ,q.ScheduledTimeIntervalInMinutes
       ,q.AsyncEventId
       ,q.ProcessTimeIntervalInMinutes
          ,q.OrganizationId
       ,q.CrmItemsBacklog AS 'Items left in Mailbox after Process Cycle'
       ,q.MailboxId
          ,q.MailboxProcessCompletedOn 
*/

FROM    (SELECT ItemsFailed
               ,MailboxProcessStartedOn
               ,OrganizationId
               ,CrmItemsBacklog
               ,MailboxId
               ,MachineName
               ,MailboxStatisticsId
               ,MailboxProcessCompletedOn
               ,OperationTypeId
               ,ProcessResult
               ,ItemsProcessed
               ,MailboxProcessScheduledOn
               ,IndividualStepDurations
               ,ScheduledTimeIntervalInMinutes
               ,AsyncEventId
               ,ProcessTimeIntervalInMinutes
               ,MailboxProcessStartedOn_Prev = LAG(MailboxProcessStartedOn) OVER (PARTITION BY MailboxId ORDER BY MailboxProcessStartedOn)  

/* THE WHERE CLAUSE DEFINES THE MINUTES PASSED BETWEEN EACH PROCESS EVENT FOR EACH MAILBOX
   For example: 60 will pull 2 events in which the second processing took more then 60 minutes after the previous one */

FROM   MailboxStatisticsBase) AS q join Mailbox M on q.MailboxId = m.MailboxId
WHERE   DATEDIFF(MINUTE, q.MailboxProcessStartedOn_prev, q.MailboxProcessStartedOn) > 60

/*
//    ADD MAILBOX ID IF YOU WANT TO SEE CYCLES ON SPECIFIC MAILBOX:
//       and q.MailboxId = 'MailboxID' 
*/

/*
//  ADD OPERATIONTYPEID TO FILTER INCOMING/OUTGOING/ACT PROCCESSING  ***  INCOMING= 0 | OUTGOING= 1  |  ACT= 2
//   and q.OperationTypeId = 0 
*/
ORDER BY q.MailboxProcessStartedOn desc
