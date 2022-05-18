SELECT DISTINCT --trans.publicid as TransPublicID,
                claim.claimnumber            AS "Claim#" ,
                Cast(claim.lossdate AS DATE) AS "Date of Loss" ,
                CASE
                                WHEN adjuster.publicid IS NULL THEN NULL
                                ELSE adjustercon.firstname
                                                                ||'-'
                                                                ||adjustercon.lastname
                END AS "Claim Owning Adjuster" ,
                CASE
                                WHEN expowner.publicid IS NULL THEN NULL
                                ELSE expownercon.firstname
                                                                ||'-'
                                                                ||expownercon.lastname
                END AS "Feature Owning Adjuster" ,
                CASE
                                WHEN payreq.publicid IS NULL THEN NULL
                                ELSE payreqcon.firstname
                                                                ||'-'
                                                                ||payreqcon.lastname
                END AS "Payment Requestor" ,
                CASE
                                WHEN approver.publicid IS NULL THEN NULL
                                ELSE approvercon.firstname
                                                                ||'-'
                                                                ||approvercon.lastname
                END                              AS "Payment Approver" ,
                Cast (trans.bookingdate AS    DATE) AS "Date of payment request" ,
                Cast(transset.approvaldate AS DATE) AS "Date payment approved" ,
                feature.publicid                    AS "GWCC Feature ID" ,
                payee.payee_1                       AS "Payee1" ,
                payee.payee_2                       AS "Payee2" ,
                payee.payee_3                       AS "Payee3" ,
                ctype.NAME                          AS "Feature Type" ,
                lineitem.reportingamount            AS "PaymentAmount" ,
                trans.publicid                      AS "GWCC PaymentID" ,
                clmcheck.insurpaytrackingnum_acc    AS "PaymentID from OneInc" ,
                pymnttype.NAME                      AS "Payment Type" ,
                insurpmethod.NAME                   AS "InsurPayMethod" ,
                chkpaymethod.NAME                   AS "DisbursementType" ,
                clmcheck.checknumber                AS "Check#"
                --, "DisbursementStatus"
                ,
                transtat.typecode   AS "Payment Status" ,
                clmchkstat.typecode AS "Check Status" -- all kind of status including cheque status
                ---, transactionstatus all status other than check status ---
                ,
                CASE
                                WHEN Lower(clmchkstat.typecode) = 'cleared'
                                AND             trans.checkid IS NOT NULL THEN Cast(trans.updatetime AS DATE)
                                ELSE NULL
                END AS "Date Disbursed" ,
                CASE
                                WHEN Lower(clmchkstat.typecode) = 'stopped' THEN Cast(clmcheck.updatetime AS DATE)
                                ELSE NULL
                END AS "Date Of Stop Payment" -- only for Check
                ,
                CASE
                                WHEN Lower(transtat.typecode) = 'stopped' THEN 'Y'
                                ELSE 'N'
                END AS "Stop Payment Status"
FROM            (
                          SELECT    lineitem.publicid AS lineitem_publicid ,
                                    trans.publicid    AS transaction_publicid ,
                                    CASE
                                              WHEN transtype.typecode = 'Payment' THEN
                                                        CASE
                                                                  WHEN transset.approvaldate > clmcheck.scheduledsenddate
                                                                  AND       transset.approvaldate > lineitem.createtime THEN transset.approvaldate
                                                                  WHEN clmcheck.scheduledsenddate > lineitem.createtime
                                                                  AND       clmcheck.scheduledsenddate > transset.approvaldate THEN clmcheck.scheduledsenddate
                                                                  ELSE lineitem.createtime
                                                        END
                                              WHEN transtype.typecode IN ('Reserve',
                                                                          'RecoveryReserve') THEN
                                                        CASE
                                                                  WHEN transset.approvaldate > lineitem.createtime THEN transset.approvaldate
                                                                  ELSE lineitem.createtime
                                                        END
                                              ELSE lineitem.createtime
                                    END AS trans_proc_dts ,
                                    pol.policysystemperiodid
                          FROM      claims_merged_new.cc_transactionlineitem lineitem
                          JOIN      claims_merged_new.cc_transaction trans
                          ON        lineitem.transactionid = trans.id
                          JOIN      claims_merged_new.cc_transactionset transset
                          ON        trans.transactionsetid = transset.id
                          LEFT JOIN claims_merged_new.cc_check clmcheck
                          ON        trans.checkid = clmcheck.id
                          LEFT JOIN claims_merged_new.cctl_transactionlifecyclestate state
                          ON        trans.lifecyclestate = state.id
                          LEFT JOIN claims_merged_new.cctl_transaction transtype
                          ON        trans.subtype = transtype.id
                          LEFT JOIN claims_merged_new.cctl_transactionstatus transtat
                          ON        trans.status = transtat.id
                          LEFT JOIN claims_merged_new.cc_claim claim
                          ON        trans.claimid = claim.id
                          LEFT JOIN claims_merged_new.cc_policy pol
                          ON        claim.policyid = pol.id
                          WHERE     (
                                              transtype.typecode = 'Payment'
                                    AND       ( (
                                                                  lineitem.createtime > '2000-01-01 00:00:00' -- Week start day(Monday 12AM) -- Today start time (Yesterday 5PM)
                                                        AND       lineitem.createtime <= '2099-12-31 23:59:59') -- Week end day(Sunday 11.59PM) -- Today end time (Today 4.59PM)
                                              OR        (
                                                                  clmcheck.scheduledsenddate> '2000-01-01 00:00:00'
                                                        AND       clmcheck.scheduledsenddate<= '2099-12-31 23:59:59')
                                              OR        (
                                                                  transset.approvaldate > '2000-01-01 00:00:00'
                                                        AND       transset.approvaldate <= '2099-12-31 23:59:59') ) ) ) AS losstranscdc
JOIN            claims_merged_new.cc_transactionlineitem lineitem
ON              losstranscdc.lineitem_publicid = lineitem.publicid
JOIN            claims_merged_new.cc_transaction trans
ON              lineitem.transactionid = trans.id
JOIN            claims_merged_new.cc_transactionset transset
ON              trans.transactionsetid = transset.id
JOIN            claims_merged_new.cc_claim claim
ON              trans.claimid = claim.id --and  claimnumber = '000-00-100010'
JOIN
                (
                         SELECT   claimid,
                                  Max(
                                  CASE
                                           WHEN rnk = 1 THEN payee_nm
                                           ELSE NULL
                                  END ) AS payee_1,
                                  Max(
                                  CASE
                                           WHEN rnk = 2 THEN payee_nm
                                           ELSE NULL
                                  END ) AS payee_2,
                                  Max(
                                  CASE
                                           WHEN rnk = 3 THEN payee_nm
                                           ELSE NULL
                                  END ) AS payee_3
                         FROM    (
                                           SELECT   payee_nm,
                                                    claimid ,
                                                    updatetime,
                                                    Row_number() OVER(partition BY claimid ORDER BY beanversion DESC) AS rnk
                                           FROM     (
                                                                    SELECT DISTINCT
                                                                    ON (
                                                                                                    claimid, payee_nm) payee_nm ,
                                                                                    claimid,
                                                                                    updatetime,
                                                                                    beanversion ,
                                                                                    Dense_rank () OVER(partition BY payee_nm,claimid ORDER BY updatetime DESC ) AS rnk
                                                                    FROM            (
                                                                                           SELECT che.claimid,
                                                                                                  che.updatetime ,
                                                                                                         Concat(Concat(COALESCE (cc.personfirstnamedenorm,'') ,' ') ,COALESCE (cc.personlastnamedenorm, '')) AS payee_nm ,
                                                                                                  cc.beanversion
                                                                                           FROM   claims_merged_new.cc_transaction trans
                                                                                           JOIN   claims_merged_new.cc_check che
                                                                                           ON     trans.checkid = che.id
                                                                                           JOIN   claims_merged_new.cc_checkpayee chepay
                                                                                           ON     chepay.checkid = che.id
                                                                                           JOIN   claims_merged_new.cc_claimcontact cc
                                                                                           ON     chepay.claimcontactid = cc.id ) S1
                                                                    ORDER BY        claimid,
                                                                                    payee_nm,
                                                                                    updatetime DESC ,
                                                                                    beanversion DESC ) S2 ) payee
                         GROUP BY claimid
                         ORDER BY claimid) payee
ON              payee.claimid = claim.id
LEFT JOIN       claims_merged_new.cc_user adjuster
ON              adjuster.id = claim.assigneduserid
LEFT JOIN       claims_merged_new.cc_contact adjustercon
ON              adjuster.contactid = adjustercon.id
LEFT JOIN       claims_merged_new.cc_exposure feature
ON              trans.exposureid = feature.id
LEFT JOIN       claims_merged_new.cc_user expowner
ON              expowner.id = feature.assigneduserid
LEFT JOIN       claims_merged_new.cc_contact expownercon
ON              expowner.contactid = expownercon.id
LEFT JOIN       claims_merged_new.cc_user payreq
ON              payreq.id = transset.requestinguserid
LEFT JOIN       claims_merged_new.cc_contact payreqcon
ON              payreq.contactid = payreqcon.id
LEFT JOIN
                (
                       SELECT id,
                              typecode approvalstatuscd
                       FROM   claims_merged_new.cctl_approvalstatus ccaprvst
                       WHERE  typecode = 'approved' ) approvalstat
ON              approvalstat.id=transset.approvalstatus
LEFT JOIN
                (
                          SELECT    ccacttype.typecode      typecode,
                                    ccact.assigneduserid   AS assigneduserid,
                                    ccact.transactionsetid    transactionsetid
                          FROM      claims_merged_new.cc_activity ccact
                          LEFT JOIN claims_merged_new.cctl_activitytype ccacttype
                          ON        ccacttype.id = ccact.type
                          WHERE     ccacttype.typecode ='approval' ) AS actvty
ON              transset.id = actvty.transactionsetid
LEFT JOIN       claims_merged_new.cc_user approver
ON              approver.id = actvty.assigneduserid
LEFT JOIN       claims_merged_new.cc_contact approvercon
ON              approver.contactid = approvercon.id
LEFT JOIN       claims_merged_new.cc_contact clmnt
ON              feature.claimantdenormid = clmnt.id
LEFT JOIN       claims_merged_new.cc_contact clmntalt
ON              claim.claimantdenormid = clmntalt.id
LEFT JOIN       claims_merged_new.cc_coverage cov
ON              feature.coverageid = cov.id
LEFT JOIN       claims_merged_new.cc_riskunit risk
ON              cov.riskunitid = risk.id
LEFT JOIN       claims_merged_new.cc_policylocation polloc
ON              risk.policylocationid = polloc.id
LEFT JOIN       claims_merged_new.cc_policylocation garloc
ON              risk.vehiclelocationid = garloc.id
LEFT JOIN       claims_merged_new.cc_check clmcheck
ON              trans.checkid = clmcheck.id
AND             clmcheck.claimid = transset.claimid
LEFT JOIN       claims_merged_new.cc_checkpayee chkpayee
ON              chkpayee.checkid = clmcheck.id
LEFT JOIN       claims_merged_new.cc_claimcontact claimco
ON              chkpayee.claimcontactid = claimco.id
LEFT JOIN       claims_merged_new.cc_contact payeecon
ON              payeecon.id = chkpayee.payeedenormid
                --typelists
LEFT JOIN       claims_merged_new.cctl_coveragetype ctype
ON              feature.primarycoverage = ctype.id
LEFT JOIN       claims_merged_new.cctl_costtype costtype
ON              trans.costtype = costtype.id
LEFT JOIN       claims_merged_new.cctl_costcategory costcat
ON              trans.costcategory = costcat.id
LEFT JOIN       claims_merged_new.cctl_lobcode lobtype
ON              claim.lobcode = lobtype.id
LEFT JOIN       claims_merged_new.cctl_transaction transtype
ON              trans.subtype = transtype.id
LEFT JOIN       claims_merged_new.cctl_recoverycategory recov
ON              trans.recoverycategory = recov.id
LEFT JOIN       claims_merged_new.cctl_currency currency
ON              trans.currency = currency.id
LEFT JOIN       claims_merged_new.cctl_paymenttype pymnttype
ON              trans.paymenttype = pymnttype.id
LEFT JOIN       claims_merged_new.cctl_exposuretype featuretype
ON              featuretype.id = feature.exposuretype
LEFT JOIN       claims_merged_new.cctl_coveragesubtype covsubtype
ON              covsubtype.id = feature.coveragesubtype
LEFT JOIN       claims_merged_new.cctl_insurpaymethod_acc insurPmethod
ON              insurpmethod.id = clmcheck.insurpaymethod_acc
LEFT JOIN       claims_merged_new.cctl_transactionstatus transtat
ON              trans.status = transtat.id
LEFT JOIN       claims_merged_new.cctl_transactionstatus clmchkstat
ON              clmcheck.status = clmchkstat.id
LEFT JOIN       claims_merged_new.cctl_paymentmethod chkpaymethod
ON              clmcheck.paymentmethod = chkpaymethod.id
from           : sourav sarkar (americas 2 - ideas-d&c)SENT: tuesday,
                may 17,
                2022 8:53 pmTO: chittaranjan umakanth bhat (americas 2 - ideas-apps & data) <chittaranjan.bhat@wipro.com>CC: ramya gowda (americas 2 - ideas-d&c) <ramya.gowda1@wipro.com>SUBJECT: customerloss -- Weekly & monthly
                WITH featurecdc AS
                (
                         SELECT   id ,
                                  min(updatetime) AS updatetime
                         FROM     (
                                         SELECT exp.id         AS id,
                                                exp.updatetime AS updatetime
                                         FROM   claims_merged_new.cc_exposure exp
                                         JOIN   claims_merged_new.cc_claim claim
                                         ON     claim.id = exp.claimid
                                         JOIN   claims_merged_new.cctl_claimstate cstate
                                         ON     claim.state = cstate.id
                                         WHERE  exp.updatetime > '2000-01-01 00:00:00'
                                         AND    exp.updatetime <= '2099-12-31 23:59:59'
                                         AND    cstate.typecode <> 'draft'
                                         UNION
                                         SELECT exp.id         AS id,
                                                exp.createtime AS updatetime
                                         FROM   claims_merged_new.cc_exposure exp
                                         JOIN   claims_merged_new.cc_claim claim
                                         ON     claim.id = exp.claimid
                                         JOIN   claims_merged_new.cctl_claimstate cstate
                                         ON     claim.state = cstate.id
                                         WHERE  exp.createtime > '2000-01-01 00:00:00'
                                         AND    exp.createtime <= '2099-12-31 23:59:59'
                                         AND    cstate.typecode <> 'draft'
                                         UNION
                                         SELECT exp.id,
                                                ind.updatetime
                                         FROM   claims_merged_new.cc_exposure exp
                                         JOIN   claims_merged_new.cc_claim claim
                                         ON     claim.id = exp.claimid
                                         JOIN   claims_merged_new.cctl_claimstate cstate
                                         ON     claim.state = cstate.id
                                         JOIN   claims_merged_new.cc_claimindicator ind
                                         ON     exp.claimid = ind.claimid
                                         JOIN   claims_merged_new.cctl_claimindicator indtype
                                         ON     ind.subtype = indtype.id
                                         AND    indtype.typecode = 'LitigationClaimIndicator'
                                         WHERE  ind.updatetime > '2000-01-01 00:00:00'
                                         AND    ind.updatetime <= '2099-12-31 23:59:59'
                                         AND    exp.createtime <= '2099-12-31 23:59:59'
                                         AND    cstate.typecode <> 'draft') q1
                         GROUP BY id ) ,
                claim AS
                (
                       SELECT claim.id                          AS claim_id ,
                              claim.claimnumber                 AS claimnumber ,
                              claim.nicbreferral_ext            AS nicbreferral_ext ,
                              cast(claim.lossdate AS timestamp) AS loss_date ,
                              cast(claim.reporteddate AS date)  AS reported_date ,
                              cast(claim.createtime AS   date)  AS date_entered ,
                              cast(claim.closedate AS    date)  AS closed_date ,
                              cast(claim.reopendate AS   date)  AS reopened_date ,
                              (
                                     SELECT fltrating.NAME
                                     FROM   claims_merged_new.cctl_faultrating fltrating
                                     WHERE  claim.faultrating = fltrating.id) AS at_fault ,
                              claim.fault                                     AS at_faultpercent ,
                              claim.description                               AS accident_facts ,
                              CASE
                                     WHEN claim.catastropheid IS NULL THEN 'N'
                                     ELSE 'Y'
                              END AS catastrophe_flag ,
                              claim.lossdate ,
                              (
                                     SELECT lcause.NAME
                                     FROM   claims_merged_new.cctl_losscause lcause
                                     WHERE  claim.losscause = lcause.id) AS losscause ,
                              (
                                     SELECT ltype.NAME
                                     FROM   claims_merged_new.cctl_losstype ltype
                                     WHERE  claim.losstype = ltype.id) AS losstype ,
                              claim.losslocationid                     AS clm_losslocationid ,
                              (
                                     SELECT clmjurisstate.NAME
                                     FROM   claims_merged_new.cctl_jurisdiction clmjurisstate
                                     WHERE  claim.jurisdictionstate = clmjurisstate.id ) AS claim_jurisstate ,
                              (
                                     SELECT cstate.NAME
                                     FROM   claims_merged_new.cctl_claimstate cstate
                                     WHERE  cstate.id = claim.state) AS claim_status ,
                              claim.claimantdenormid                 AS clmclaimantdenormid ,
                              (
                                     SELECT catastrophenumber
                                     FROM   claims_merged_new.cc_catastrophe cat
                                     WHERE  claim.catastropheid = cat.id) AS cat_code ,
                              claim.assigneduserid                        AS clm_assigneduserid ,
                              claim.insureddenormid                       AS clm_insureddenormid ,
                              (
                                     SELECT NAME
                                     FROM   claims_merged_new.cctl_lobcode lob
                                     WHERE  lob.id = claim.lobcode) AS clm_lobcode ,
                              claim.salvagestatus                   AS clm_salvagestatus ,
                              claim.siustatus ,
                              (
                                     SELECT siusstat.typecode
                                     FROM   claims_merged_new.cctl_siustatus siusstat
                                     WHERE  siusstat.id = claim.siustatus) AS clm_siustatus ,
                              claim.siureviewstatus_ext ,
                              (
                                     SELECT siurevstat.typecode
                                     FROM   claims_merged_new.cctl_siureviewstatus_ext siurevstat
                                     WHERE  siurevstat.id = claim.siureviewstatus_ext) AS clm_siureviewstat ,
                              claim.siulifecyclestate                                  AS clm_siulifecyclestate ,
                              claim.isostatus                                          AS clm_isostatus ,
                              claim.litigationstatus                                   AS clm_litigationstatus ,
                              claim.policyid                                           AS claim_policyid ,
                              claim.coverageinquestion ,
                              CASE
                                     WHEN
                                            (
                                                   SELECT salvagestat.typecode
                                                   FROM   claims_merged_new.cctl_salvagestatus salvagestat
                                                   WHERE  salvagestat.id = claim.salvagestatus ) = 'Open' THEN claim.updatetime
                              END AS date_salvage_opened ,
                              CASE
                                     WHEN
                                            (
                                                   SELECT salvagestat.typecode
                                                   FROM   claims_merged_new.cctl_salvagestatus salvagestat
                                                   WHERE  salvagestat.id = claim.salvagestatus ) = 'Closed' THEN claim.updatetime
                              END AS date_salvage_closed ,
                              CASE
                                     WHEN claim.coverageinquestion IS NOT NULL THEN 'Y'
                                     ELSE 'N'
                              END AS clm_coverageinquestion
                       FROM   claims_merged_new.cc_claim claim ) ,
                feature AS
                (
                       SELECT exp.id                        AS feature_id ,
                              exp.publicid                  AS feature_publicid ,
                              exp.claimid                   AS feature_claimid ,
                              cast (exp.createtime AS date) AS feature_opened_date ,
                              cast (exp.updatetime AS date) AS feature_updatetime ,
                              (
                                     SELECT expcloutcome.NAME
                                     FROM   claims_merged_new.cctl_exposureclosedoutcometype expcloutcome
                                     WHERE  expcloutcome.id = exp.closedoutcome) AS exp_outcome ,
                              (
                                     SELECT expjurisstate.NAME
                                     FROM   claims_merged_new.cctl_jurisdiction expjurisstate
                                     WHERE  exp.jurisdictionstate = expjurisstate.id ) AS exp_jurisstate ,
                              cast(exp.closedate AS date)                              AS feature_closed_date ,
                              exp.assigneduserid                                       AS exp_assigneduserid ,
                              exp.claimantdenormid                                     AS exp_claimantdenormid ,
                              exp.coverageid ,
                              exp.incidentid ,
                              exp.templocationid AS exp_templocationid ,
                              exp.claimanttype      exp_claimanttype ,
                              (
                                     SELECT expcovtype.NAME
                                     FROM   claims_merged_new.cctl_coveragetype expcovtype
                                     WHERE  expcovtype.id = exp.primarycoverage ) AS exp_primarycovg ,
                              (
                                     SELECT exptype.NAME
                                     FROM   claims_merged_new.cctl_exposuretype exptype
                                     WHERE  exptype.id = exp.exposuretype ) AS exposuretype ,
                              exp.coveragesubtype                              exp_coveragesubtype ,
                              exp.exposuretier                                 exp_exposuretier ,
                              exp.exposuretype                                 exp_exposuretype ,
                              exp.losscategory                                 exp_losscategory ,
                              exp.lossparty                                    exp_lossparty ,
                              exp.lostpropertytype                             exp_lostpropertytype ,
                              exp.losscategory                                 exp_losscategory ,
                              (
                                     SELECT expstate.NAME
                                     FROM   claims_merged_new.cctl_exposurestate expstate
                                     WHERE  expstate.id = exp.state ) AS exp_status
                       FROM   claims_merged_new.cc_exposure exp ) ,
                contact AS
                (
                       SELECT con.id       AS contact_id ,
                              con.publicid AS con_publicid ,
                              con.primaryaddressid ,
                              COALESCE(con.NAME, COALESCE(con.firstname,'')
                                     ||' '
                                     || COALESCE(con.lastname,''))                   AS contact_name ,
                              COALESCE(con.firstname,'')                             AS con_first_name ,
                              COALESCE(con.lastname,'')                              AS con_last_name ,
                              cast (con.dateofbirth AS date)                         AS dob ,
                              date_trunc('year',age(CURRENT_DATE , con.dateofbirth)) AS con_age ,
                              (
                                     SELECT congender.NAME
                                     FROM   claims_merged_new.cctl_gendertype congender
                                     WHERE  congender.id = con.gender) AS con_gender ,
                              (
                                     SELECT cstate.NAME
                                     FROM   claims_merged_new.cctl_state cstate
                                     WHERE  con.licensestate = cstate.id) AS con_licensestate ,
                              con.licensenumber
                       FROM   claims_merged_new.cc_contact con ) ,
                address AS
                (
                       SELECT addr.id       AS address_id ,
                              addr.publicid AS addr_publicid ,
                              COALESCE(addr.addressline1,'')
                                     ||' '
                                     || COALESCE(addr.addressline2,'')
                                     || ' '
                                     || COALESCE(addr.addressline3,'') AS addr_addressline ,
                              addr.city                                AS addr_city ,
                              (
                                     SELECT addrstate.NAME
                                     FROM   claims_merged_new.cctl_state addrstate
                                     WHERE  addr.state = addrstate.id) AS addr_state ,
                              addr.postalcode                          AS addr_zip ,
                              (
                                     SELECT NAME
                                     FROM   claims_merged_new.cctl_addresstype addrtype
                                     WHERE  addr.addresstype = addrtype.id) AS address_type
                       FROM   claims_merged_new.cc_address addr ) ,
                contactaddress AS
                (
                         SELECT   s2.contactid                      conaddr_contactid,
                                  string_agg(s2.conaddress, ' || ') allconaddress
                         FROM     (
                                                  SELECT DISTINCT (s1.contactid, s1.conaddress) conidaddr,
                                                                  s1.contactid,
                                                                  s1.addressid,
                                                                  s1.conaddress
                                                  FROM            (
                                                                         SELECT conaddr.contactid ,
                                                                                conaddr.addressid ,
                                                                                (
                                                                                       SELECT NAME
                                                                                       FROM   claims_merged_new.cctl_addresstype addrtype
                                                                                       WHERE  addr.addresstype = addrtype.id)
                                                                                       || '- '
                                                                                       || COALESCE(addr.addressline1,'')
                                                                                       ||' '
                                                                                       || COALESCE(addr.addressline2,'')
                                                                                       || ' '
                                                                                       || COALESCE(addr.addressline3,'')
                                                                                       || ' '
                                                                                       || COALESCE(addr.city,'')
                                                                                       ||' '
                                                                                       || COALESCE(
                                                                                                    (
                                                                                                    SELECT addrstate.NAME
                                                                                                    FROM   claims_merged_new.cctl_state addrstate
                                                                                                    WHERE  addr.state = addrstate.id),'')
                                                                                       || ' '
                                                                                       || COALESCE(addr.postalcode,'') AS conaddress
                                                                         FROM   claims_merged_new.cc_contactaddress conaddr
                                                                         JOIN   claims_merged_new.cc_address addr
                                                                         ON     addr.id = conaddr.addressid ) s1
                                                  ORDER BY        s1.contactid,
                                                                  s1.addressid ) s2
                         GROUP BY s2.contactid ) ,
                contactrole AS
                (
                       SELECT claimant.claimid   AS ccr_claimid ,
                              claimant.contactid AS ccr_contactid ,
                              clmccr.claimcontactid ,
                              ccr.typecode AS clmcontactrole ,
                              ccr.NAME     AS ccr_name ,
                              claimant.claimantflag
                       FROM   claims_merged_new.cc_claimcontact claimant
                       JOIN   claims_merged_new.cc_claimcontactrole clmccr
                       ON     clmccr.claimcontactid = claimant.id
                       JOIN   claims_merged_new.cctl_contactrole ccr
                       ON     ccr.id = clmccr.role ) ,
                incident AS
                (
                       SELECT inc.id          AS incident_id ,
                              inc.claimid     AS inc_claimid ,
                              inc.description AS party_facts ,
                              inc.subtype     AS inc_subtype ,
                              inctype.NAME    AS incidentsubtype_name ,
                              CASE
                                     WHEN inctype.NAME = 'PropertyIncident' THEN 'PropertyIncident'
                                     WHEN inctype.NAME = 'MobilePropertyIncident' THEN 'MobilePropertyIncident'
                                     WHEN inctype.NAME = 'VehicleIncident' THEN 'VehicleIncident'
                                     WHEN inctype.NAME = 'FixedPropertyIncident' THEN 'FixedPropertyIncident'
                                     WHEN inctype.NAME = 'InjuryIncident' THEN 'InjuryIncident'
                              END AS incidentsubtype ,
                              inctype.priority ,
                              inc.vehicleid inc_vehid ,
                              inc.appraisal ,
                              inc.rentalbegindate ,
                              inc.rentalenddate ,
                              inc.totalloss ,
                              inc.salvagenet ,
                              (
                                     SELECT dit.NAME
                                     FROM   claims_merged_new.cctl_detailedinjurytype dit
                                     WHERE  dit.id = inc.detailedinjurytype ) AS injurytype
                              --, Sum (case when inc.VehicleID is not null then salvagenet end) over (partition by inc.claimid order by inc.ID) as claim_TotalNEtSalvage
                       FROM   claims_merged_new.cc_incident inc
                       JOIN   claims_merged_new.cctl_incident inctype
                       ON     inctype.id = inc.subtype ) ,
                bodypart AS
                (
                          SELECT    bodypart.incidentid AS bodypart_incidentid ,
                                    bodypart.primarybodypart ,
                                    primbp.NAME    AS primarybodypart_name ,
                                    dtlbptype.NAME AS detailedbodyparttype ,
                                    dtdbpdesc.NAME AS detailedbodypartdesc
                          FROM      claims_merged_new.cc_bodypart bodypart
                          LEFT JOIN claims_merged_new.cctl_bodyparttype primbp
                          ON        primbp.id = bodypart.primarybodypart
                          LEFT JOIN claims_merged_new.cctl_detailedbodyparttype dtlbptype
                          ON        dtlbptype.id = bodypart.detailedbodypart
                          LEFT JOIN claims_merged_new.cctl_detailedbodypartdesc dtdbpdesc
                          ON        dtdbpdesc.id = bodypart.detailedbodypartdesc ) ,
                clmtransaction AS
                (
                          SELECT    trans.claimid AS trans_claimid
                                    --, trans.publicID as transPublicID
                                    ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(recov.typecode) = 'SUBRO'
                                                        AND       upper(transtype.typecode) IN ('RECOVERY',
                                                                                                'RECOVERYRESERVE') ) THEN lineitem.reportingamount
                                    END) AS subrogation_recovery ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode) IN ('DCCEXPENSE',
                                                                                               'UNSPECIFIED')
                                                        AND       upper(transtype.typecode) IN ('PAYMENT',
                                                                                                'RECOVERY',
                                                                                                'TRANSACTION')) THEN lineitem.reportingamount
                                    END) AS dcce_paid ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('DCCEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END) AS dcce_reserve ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('AOEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END) aoe_reserve ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('DCCEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END) AS dcce_reserved ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('AOEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END) AS aoe_recovered ,
                                    sum(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('DCCEEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RECOVERY'))THEN lineitem.reportingamount
                                    END) AS dcce_recoverd ,
                                    sum(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('RESERVE',
                                                                                 'RECOVERYRESERVE') THEN lineitem.reportingamount
                                    END) AS loss_reserve ,
                                    sum(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('PAYMENT') THEN lineitem.reportingamount
                                    END) AS loss_paid ,
                                    sum(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('RECOVERY') THEN lineitem.reportingamount
                                    END) AS loss_recovered
                                    -- adding the loss_paid and loss_reserve to derive loss_incurred
                                    ,
                                    sum((COALESCE(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('PAYMENT') THEN lineitem.reportingamount
                                    END,0) + COALESCE(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('RESERVE',
                                                                                 'RECOVERYRESERVE') THEN lineitem.reportingamount
                                    END,0))) AS loss_incurred ,
                                    sum(
                                    CASE
                                              WHEN upper(costtype.typecode)       IN ('AOEXPENSE')
                                              AND       upper(transtype.typecode) IN ('PAYMENT',
                                                                                      'RECOVERY',
                                                                                      'TRANSACTION') THEN lineitem.reportingamount
                                    END) AS aoe_paid
                                    -- adding the AOE_paid and AOE_Reserve to derive AOE_incurred
                                    ,
                                    sum((COALESCE(
                                    CASE
                                              WHEN upper(costtype.typecode)       IN ('AOEXPENSE')
                                              AND       upper(transtype.typecode) IN ('PAYMENT',
                                                                                      'RECOVERY',
                                                                                      'TRANSACTION') THEN lineitem.reportingamount
                                    END,0)+COALESCE(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('AOEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END,0))) AS aoe_incurred ,
                                    sum(COALESCE(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode)  IN ('DCCEXPENSE')
                                                        AND       upper(transtype.typecode) IN ('RESERVE',
                                                                                                'RECOVERYRESERVE')) THEN lineitem.reportingamount
                                    END,0)+COALESCE(
                                    CASE
                                              WHEN (
                                                                  upper(costtype.typecode) IN ('DCCEXPENSE',
                                                                                               'UNSPECIFIED')
                                                        AND       upper(transtype.typecode) IN ('PAYMENT',
                                                                                                'RECOVERY',
                                                                                                'TRANSACTION')) THEN lineitem.reportingamount
                                    END,0)-COALESCE(
                                    CASE
                                              WHEN upper(transtype.typecode) IN ('RECOVERY') THEN lineitem.reportingamount
                                    END,0)) AS dcce_incurred
                          FROM      claims_merged_new.cc_transaction trans
                          JOIN      claims_merged_new.cc_transactionlineitem lineitem
                          ON        lineitem.transactionid = trans.id
                          LEFT JOIN claims_merged_new.cctl_transaction transtype
                          ON        trans.subtype = transtype.id
                          LEFT JOIN claims_merged_new.cctl_transactionstatus transtat
                          ON        trans.status = transtat.id
                          LEFT JOIN claims_merged_new.cctl_recoverycategory recov
                          ON        trans.recoverycategory = recov.id
                          LEFT JOIN claims_merged_new.cctl_costtype costtype
                          ON        trans.costtype = costtype.id
                          GROUP BY  trans.claimid ) ,
                litigation AS
                (
                       SELECT litstatustypeline.id        AS litigationid ,
                              litimatter.claimid          AS litigation_claimid ,
                              ltgtnstat.NAME              AS litigation_status ,
                              litstatustypeline.startdate AS litigation_opened_date ,
                              litstatustypeline.completiondate ,
                              CASE
                                     WHEN ltgtnstat.typecode = 'closed' --and lower(litimatterstat.typecode)='closed'
                                     THEN litstatustypeline.completiondate
                                     ELSE NULL
                              END AS litigation_closed_date
                       FROM   claims_merged_new.cc_litstatustypeline litstatustypeline
                       JOIN   claims_merged_new.cctl_litigationstatus ltgtnstat
                       ON     ltgtnstat.id = litstatustypeline.litigationstatus
                       JOIN   claims_merged_new.cc_matter litimatter
                       ON     litstatustypeline.matterid = litimatter.id
                              --join claims_merged_new.cctl_matterstatus litimatterstat on litimatterstat.id = litimatter.status
                       WHERE  EXISTS
                              (
                                     SELECT 1
                                     FROM   claims_merged_new.cc_claimindicator ind
                                     JOIN   claims_merged_new.cctl_claimindicator indtype
                                     ON     ind.subtype = indtype.id
                                     AND    indtype.typecode = 'LitigationClaimIndicator'
                                     WHERE  ind.claimid = litimatter.claimid) ) ,
                subrogation AS
                (
                         SELECT   subro.exposureid AS subro_featureid ,
                                  min(
                                  CASE
                                           WHEN
                                                    (
                                                           SELECT subrostat.typecode
                                                           FROM   claims_merged_new.cctl_subrogationstatus subrostat
                                                           WHERE  subrostat.id = subro.status ) = 'open' THEN subro.createtime
                                  END ) AS date_subro_open ,
                                  max(
                                  CASE
                                           WHEN
                                                    (
                                                           SELECT subrostat.typecode
                                                           FROM   claims_merged_new.cctl_subrogationstatus subrostat
                                                           WHERE  subrostat.id = subro.status ) = 'closed' THEN subro.closedate
                                  END) AS date_subro_close
                         FROM     claims_merged_new.cc_subrogation subro
                         WHERE    subro.exposureid IS NOT NULL
                         GROUP BY subro.exposureid )SELECT DISTINCT feature.feature_publicid    feature_id ,
                       claim.claimnumber        AS claim_number,
                       CASE
                                       WHEN claim.nicbreferral_ext IS NOT NULL THEN 'Y'
                                       ELSE 'N'
                       END AS nicb_referral,
                       claim.loss_date,
                       claim.reported_date,
                       -- claim owner , feature owner
                       claim.date_entered,
                       claim.closed_date,
                       claim.reopened_date,
                       policy.policy_number,
                       policy.policy_start_date,
                       policy.policy_end_date,
                       policy.policy_status,
                       --policy.policy_renewal_num,
                       adjcon.contact_name    AS feature_handler,
                       claim.losscause        AS loss_causation,
                       claim.losstype         AS loss_type,
                       claim.claim_status     AS claim_status,
                       claim.claim_jurisstate AS claim_state,
                       claim.catastrophe_flag,
                       claim.clm_lobcode                AS lob,
                       claim.at_fault                   AS at_fault,
                       claim.at_faultpercent            AS at_faultpercent,
                       claim.accident_facts             AS accident_facts,
                       clmlosslocation.addr_addressline AS accident_location,
                       clmlosslocation.addr_city        AS accident_city,
                       clmlosslocation.addr_state       AS accident_city,
                       clmlosslocation.addr_zip         AS accident_zip,
                       claimant.con_first_name          AS claimant_first_name,
                       claimant.con_last_name           AS claimant_last_name ,
                       claimantaddr.addr_addressline    AS claimant_address1,
                       claimantaddr.addr_city           AS claimant_city,
                       claimantaddr.addr_state          AS claimant_state,
                       claimantaddr.addr_zip            AS claimant_zip,
                       contactaddress.allconaddress     AS alladdress,
                       --Claimant.con_Age as Claimant_Age,
                       CASE
                                       WHEN claimant.con_age IS NOT NULL THEN Replace(Lower(Cast(claimant.con_age AS TEXT)),'years','') --(substring(cast(Claimant.con_Age as TEXT) , 1 , position(' ' in cast(claimant.con_Age as TEXT))-1))
                       END                 AS claimant_age,
                       claimant.con_gender AS claimant_sex ,
                       (
                              SELECT
                                     CASE
                                            WHEN feature.exp_claimanttype IN
                                                                              (
                                                                              SELECT DISTINCT id
                                                                              FROM            claims_merged_new.cctl_claimanttype cc
                                                                              WHERE           Lower(typecode) LIKE 'veh_%_driver') THEN claimant.con_licensestate
                                     END AS                                                 driver_lic_state
                              WHERE  claimant.contact_id = feature.exp_claimantdenormid) AS driver_lic_state,
                       --(select  cc.name from claims_merged_new.cctl_claimanttype cc where cc.id = feature.exp_ClaimantType) as Party_Type,
                       (
                              SELECT cc.NAME
                              FROM   claims_merged_new.cctl_losspartytype cc
                              WHERE  cc.id = feature.exp_lossparty) AS party_type,
                       CASE
                                       WHEN feature.exp_claimanttype IN
                                                       (
                                                              SELECT id
                                                              FROM   claims_merged_new.cctl_claimanttype cc
                                                              WHERE  typecode LIKE 'veh_%_driver') THEN 'Y'
                                       ELSE 'N'
                       END AS driver,
                       CASE
                                       WHEN feature.exp_claimanttype IN
                                                       (
                                                              SELECT id
                                                              FROM   claims_merged_new.cctl_claimanttype cc
                                                              WHERE  typecode LIKE 'veh_%_occupant') THEN 'Y'
                                       ELSE 'N'
                       END AS passenger,
                       CASE
                                       WHEN feature.exp_claimanttype =
                                                       (
                                                              SELECT id
                                                              FROM   claims_merged_new.cctl_claimanttype cc
                                                              WHERE  typecode ='bystander') THEN 'Y'
                                       ELSE 'N'
                       END                                 AS pedestrian,
                       incident.party_facts                AS party_facts,
                       incident.injurytype                 AS injury ,
                       injurytincident.inc_primarybodypart    body_part,
                       claimant.dob,
                       COALESCE(
                       CASE
                                       WHEN incident.inc_vehid IS NOT NULL THEN (
                                                       CASE
                                                                       WHEN incident.totalloss IS NOT NULL THEN 'Y'
                                                                       ELSE 'N'
                                                       END )
                       END ,'N') AS total_loss,
                       CASE
                                       WHEN incident.incidentsubtype = 'VehicleIncident' THEN incident.appraisal
                       END AS appraisal,
                       CASE
                                       WHEN incident.incidentsubtype = 'VehicleIncident' THEN incident.rentalbegindate
                       END AS rentalbegindate,
                       CASE
                                       WHEN incident.incidentsubtype = 'VehicleIncident' THEN incident.rentalenddate
                       END AS rentalenddate,
                       CASE
                                       WHEN incident.incidentsubtype = 'VehicleIncident' THEN (
                                                       CASE
                                                                       WHEN incident.rentalbegindate IS NOT NULL
                                                                       AND             (
                                                                                                       incident.rentalenddate IS NULL
                                                                                       OR              incident.rentalenddate > Now()) THEN 'Y'
                                                                       ELSE 'N'
                                                       END)
                       END                     AS rental_closed,
                       veh.year                   vehicle_year,
                       veh.make                   vehicle_make,
                       veh.model                  vehicle_model,
                       feature.exp_primarycovg    feature_name,
                       feature.feature_opened_date,
                       feature.exp_status         AS feature_status,
                       feature.feature_updatetime AS feature_status_date,
                       feature.exp_jurisstate     AS feature_state,
                       feature.exp_outcome        AS cwp_reason,
                       clmtransaction.loss_reserve,
                       clmtransaction.loss_paid,
                       clmtransaction.loss_recovered,
                       clmtransaction.loss_incurred,
                       clmtransaction.aoe_paid,
                       clmtransaction.aoe_incurred,
                       clmtransaction.dcce_paid,
                       clmtransaction.dcce_reserve,
                       clmtransaction.aoe_reserve,
                       clmtransaction.dcce_incurred,
                       clmtransaction.dcce_reserved,
                       clmtransaction.aoe_recovered,
                       clmtransaction.dcce_recoverd,
                       (
                              SELECT contactrole.claimcontactid
                              FROM   contactrole
                              WHERE  contactrole.clmcontactrole = 'insured'
                              AND    feature.exp_claimantdenormid = contactrole.claimcontactid) AS insured,
                       (
                              SELECT con_first_name
                              FROM   contact
                              WHERE  contact.contact_id =
                                     (
                                            SELECT contactrole.claimcontactid
                                            FROM   contactrole
                                            WHERE  contactrole.clmcontactrole = 'insured'
                                            AND    feature.exp_claimantdenormid = contactrole.claimcontactid) ) AS insured_first_name,
                       (
                              SELECT con_last_name
                              FROM   contact
                              WHERE  contact.contact_id =
                                     (
                                            SELECT contactrole.claimcontactid
                                            FROM   contactrole
                                            WHERE  contactrole.clmcontactrole = 'insured'
                                            AND    feature.exp_claimantdenormid = contactrole.claimcontactid) ) AS insured_last_name,
                       (
                              SELECT
                                     (
                                            SELECT address.addr_zip
                                            FROM   address
                                            WHERE  address.address_id = contact.primaryaddressid ) AS insured_zip
                              FROM   contact
                              WHERE  contact.contact_id =
                                     (
                                            SELECT contactrole.claimcontactid
                                            FROM   contactrole
                                            WHERE  contactrole.clmcontactrole = 'insured'
                                            AND    feature.exp_claimantdenormid = contactrole.claimcontactid) ) AS insured_zip,
                       veh.vin                                                                                     policy_vehicle_num,
                       COALESCE(vehowncon.NAME, COALESCE(vehowncon.firstname,'')
                                       ||' '
                                       || COALESCE(vehowncon.lastname,'')) AS vehicle_owner,
                       (
                              SELECT licensenumber
                              FROM   contact
                              WHERE  contact.contact_id =
                                     (
                                                     SELECT DISTINCT contactrole.claimcontactid
                                                     FROM            contactrole
                                                     WHERE           Lower(contactrole.clmcontactrole) = 'driver'
                                                     AND             feature.exp_claimantdenormid = contactrole.claimcontactid) ) AS policy_driver_num,
                       Cast (CURRENT_DATE AS DATE)                                                                                AS as_of,
                       claim.lossdate                                                                                             AS loss_time,
                       CASE
                                       WHEN claim.siustatus IS NOT NULL THEN (
                                                       CASE
                                                                       WHEN claim.clm_siustatus = 'No_Referral' THEN 'N'
                                                                       ELSE 'Y'
                                                       END)
                       END AS siu_referred,
                       CASE
                                       WHEN claim.siureviewstatus_ext IS NOT NULL THEN (
                                                       CASE
                                                                       WHEN claim.clm_siureviewstat = 'Accepted' THEN 'Y'
                                                                       ELSE 'N'
                                                       END)
                       END AS siu_accepted,
                       claim.date_salvage_opened,
                       claim.date_salvage_closed,
                       Sum (
                       CASE
                                       WHEN incident.inc_vehid IS NOT NULL THEN incident.salvagenet
                       END) OVER (partition BY feature.feature_id ORDER BY incident.incident_id) AS salvage_recovery_amount,
                       --case when Incident.inc_vehID is not null then Incident.salvagenet end as Salvage_Recovery_Amount,
                       CASE
                                       WHEN litigation.litigationid IS NOT NULL THEN 'Y'
                                       ELSE 'N'
                       END AS litigation_fl,
                       litigation.litigation_status,
                       litigation.litigation_opened_date ,
                       litigation.litigation_closed_date,
                       subrogation.date_subro_open  AS date_subro_open,
                       subrogation.date_subro_close AS date_subro_close,
                       clmtransaction.subrogation_recovery,
                       --feature.Feature_PublicID as GWCC_Feature_ID,
                       claim.clm_coverageinquestion AS coverageinquestion,
                       claim.cat_code,
                       feature.feature_closed_date
       FROM            featurecdc
       JOIN            feature
       ON              featurecdc.id = feature.feature_id
       JOIN            claim
       ON              claim.claim_id = feature.feature_claimid
       LEFT JOIN       clmtransaction
       ON              claim.claim_id = clmtransaction.trans_claimid
       LEFT JOIN       claims_merged_new.cc_user adjuser
       ON              feature.exp_assigneduserid = adjuser.id
       LEFT JOIN       contact Adjcon
       ON              adjuser.contactid = adjcon.contact_id
       LEFT JOIN       address clmlosslocation
       ON              claim.clm_losslocationid = clmlosslocation.address_id
       LEFT JOIN       contact Claimant
       ON              claimant.contact_id = feature.exp_claimantdenormid
       LEFT JOIN       address ClaimantAddr
       ON              claimant.primaryaddressid = claimantaddr.address_id
       LEFT JOIN       contactaddress
       ON              contactaddress.conaddr_contactid = feature.exp_claimantdenormid
       LEFT JOIN
                       (
                                       SELECT DISTINCT pol.policynumber            AS policy_number, --(case when length(policynumber) > 8 then substring(policynumber from 1 for 8)||'-'||substring(policynumber from 9 for length(policynumber)-8) else policynumber end) AS policy_number,
                                                       Cast(effectivedate AS  DATE)    policy_start_date,
                                                       Cast(expirationdate AS DATE)    policy_end_date,
                                                       id                              policy_id,
                                                       (
                                                              SELECT polstat.NAME
                                                              FROM   claims_merged_new.cctl_policystatus polstat
                                                              WHERE  polstat.id = pol.status) AS policy_status
                                       FROM            claims_merged_new.cc_policy pol ) policy
       ON              policy.policy_id = claim.claim_policyid
       LEFT JOIN       incident
       ON              incident.incident_id = feature.incidentid
       AND             incident.inc_claimid = claim.claim_id
       LEFT JOIN       claims_merged_new.cc_vehicle veh
       ON              veh.id = incident.inc_vehid
       LEFT JOIN       claims_merged_new.cc_vehicleowner vehicleowner
       ON              veh.id = vehicleowner.vehicleid
       LEFT JOIN       claims_merged_new.cc_contact vehowncon
       ON              vehicleowner.lienholderid = vehowncon.id
       LEFT JOIN       subrogation
       ON              subrogation.subro_featureid = feature.feature_id
       LEFT JOIN       litigation
       ON              litigation.litigation_claimid = claim.claim_id
       LEFT JOIN
                       (
                                SELECT   inc_claimid,
                                         incident_id,
                                         String_agg(primarybodypart_name,'||') AS inc_primarybodypart
                                FROM     (
                                                         SELECT DISTINCT (s1.inc_claimid, s1.incident_id, s1.primarybodypart_name) primarybodypart,
                                                                         s1.inc_claimid,
                                                                         s1.incident_id,
                                                                         primarybodypart_name
                                                         FROM            (
                                                                                   SELECT    incident.inc_claimid,
                                                                                             incident.incident_id,
                                                                                             bodypart.primarybodypart_name
                                                                                   FROM      incident
                                                                                   LEFT JOIN bodypart
                                                                                   ON        bodypart.bodypart_incidentid = incident.incident_id
                                                                                   WHERE     incident.incidentsubtype_name = 'InjuryIncident' ) s1
                                                         ORDER BY        inc_claimid,
                                                                         incident_id,
                                                                         primarybodypart_name) s2
                                GROUP BY inc_claimid,
                                         incident_id ) injurytincident
       ON              injurytincident.incident_id = feature.incidentid
       AND             injurytincident.inc_claimid = claim.claim_id
                       --group by Feature_PublicID having count(*)>1