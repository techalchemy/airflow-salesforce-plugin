SELECT
  id,
  ownerid,
  createdbyid,
  parentid,
  createddate,
  systemmodstamp,
  isdeleted,
  parent.recordtypeid,
  parent.type,
  title,
  body,
  isprivate,
  lastmodifiedbyid
FROM
  note
WHERE
  ((systemmodstamp >= %s AND systemmodstamp <= %s) OR isdeleted = TRUE)
  AND (
    (parent.type = 'Case' AND parent.recordtypeid = '01230000000n2AjAAI')
    OR
    (parent.type = 'PS_Engagement__c')
  )
