SELECT
   id
  ,account__c
  ,account_group__c
  ,contact__c
  ,createdbyid
  ,createddate
  ,lastmodifiedbyid
  ,lastmodifieddate
  ,systemmodstamp
FROM
  accounts_supported__c
WHERE
  (systemmodstamp >= %s AND systemmodstamp <= %s) OR isdeleted = TRUE
