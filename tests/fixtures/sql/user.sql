SELECT
   id
  ,accountid
  ,contactid
  ,country
  ,createdbyid
  ,createddate
  ,email
  ,firstname
  ,fullphotourl
  ,isactive
  ,isportalenabled
  ,lastlogindate
  ,lastmodifiedbyid
  ,lastmodifieddate
  ,lastname
  ,name
  ,profileid
  ,smallphotourl
  ,systemmodstamp
  ,title
  ,username
  ,usertype
  ,phone
FROM
  User
WHERE
  systemmodstamp >= %s AND systemmodstamp <= %s
