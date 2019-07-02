SELECT
   id
  ,accountid
  ,createdbyid
  ,createddate
  ,email
  ,fax
  ,firstname
  ,hasoptedoutofemail
  ,isdeleted
  ,lastmodifiedbyid
  ,lastmodifieddate
  ,lastname
  ,mailingcity
  ,mailingcountry
  ,mailingpostalcode
  ,mailingstate
  ,mailingstreet
  ,name
  ,ownerid
  ,phone
  ,systemmodstamp
  ,title
FROM
  Contact
WHERE
  (systemmodstamp >= %s AND systemmodstamp <= %s) OR isdeleted = TRUE
