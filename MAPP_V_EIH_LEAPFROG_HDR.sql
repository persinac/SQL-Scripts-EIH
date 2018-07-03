CREATE OR REPLACE VIEW `MAPP_V_EIH_LEAPFROG_HDR` AS
select straight_join 
concat(
	trans.EXTERNAL_ID,
	convert(
		concat('-',(
			case when (tType.R_TYPE_ID = 1) then 'MTG' 
			when (tType.R_TYPE_ID = 2) then 'CNS' 
			when (tType.R_TYPE_ID = 3) then 'EDG' 
			when (tType.R_TYPE_ID = 4) then 'RSG' 
			when (tType.R_TYPE_ID = 5) then 'CPS' 
			when (tType.R_TYPE_ID = 6) then 'CHC' 
			else 'BUD' end
		)) using utf8
	)
) AS FCPA_PROJ_ID
, 'Active' AS PROJ_ST
,trans.PROJECT_NAME AS PROJ_NM
,trim(rat.STATEMENT) AS PROJ_DESC
,tCtry.NAME AS CONTR_RGN
,(case when (tType.R_TYPE_ID = 1) then 'Meeting' 
	when (tType.R_TYPE_ID = 2) then 'Consultancy' 
	when (tType.R_TYPE_ID = 3) then 'Educational Grant' 
	when (tType.R_TYPE_ID = 4) then 'Research Grant' 
	when (tType.R_TYPE_ID = 5) then 'Corporate Sponsorship' 
	when (tType.R_TYPE_ID = 6) then 'Charitable Donation' 
	when (tType.R_TYPE_ID = 7) then 'Business Donation' 
	else 'Third Party' end) AS PROCS_ACTV
,(case when (tType.R_TYPE_ID = 1) then meet.EVENT_HOST 
	when (tType.R_TYPE_ID = 2) then convert(group_concat(distinct serviceData.ENG_LABEL separator ' | ') using utf8) 
	when (tType.R_TYPE_ID = 6) then efContrType.ENG_LABEL 
	else '' end) AS SUBPROCESS_TYP
,projectOwner.DEPARTMENT_DESC AS RQST_DEPT
,vend.VENDOR_NBR AS TRANS_PTY_VNDR_ID
,vend.NAME AS TRANS_PTY_VNDR_NAME
,ef.MDGS_EXT_ID AS PROPSD_VNDR_NM
,ef.NEW_VENDOR_NM AS PROPSD_VNDR_DTL
,(case when (tType.R_TYPE_ID = 1) then meetLog.VENUE_NAME end) AS VENUE
,(case when isnull(trans.START_DATE) 
	then trans.FIRST_START_DATE_RANGE 
	else trans.START_DATE end) AS EVENT_STRT_DT
,trans.END_DATE AS EVENT_END_DT
,(case when (tType.R_TYPE_ID = 3) then 'Educational Grant' 
	when (tType.R_TYPE_ID = 4) then 'Research Grant' 
	when (tType.R_TYPE_ID = 5) then 'Corporate Sponsorship' 
	else '' end) AS GRANT_TYP
,orgType.ENG_LABEL AS ORG_TYP
,(case 
	when (tType.R_TYPE_ID in (3,5)) then (
		case when (find_in_set('GO',group_concat(distinct rbcs.GO_STATUS separator ',')) > 0) then 1 end)
		else 0 end) AS GOV_CNTL
,(case when (tType.R_TYPE_ID in (3,4,6)) then (
	case when (trans.IS_ENTITY_BASED = 1) then 'Entity' 
	when (trans.IS_ENTITY_BASED = 0) then 'Individual' else '' end) else '' end) AS GRANT_RCPNT_TYP
,(case when (tType.R_TYPE_ID = 1) then meetLog.VENUE_ADDRESS end) AS CITY
,(case when (tType.R_TYPE_ID = 1) then meetLog.VENUE_NAME end) AS MEETINGVENUE
,(case when (trans.SOLICITED_OR_UNSOLICITED = 53) then 'Yes'
	when (trans.SOLICITED_OR_UNSOLICITED = 54) then 'No' else '' end) AS ISSOLICITED
,(case when (tType.R_TYPE_ID = 1) then group_concat(distinct brMeetData.ENG_LABEL separator ' | ') end) AS MTNG_PURP
,(case when (tType.R_TYPE_ID = 1) then 1 else 0 end) AS MTNG_TYP,(case when (tType.R_TYPE_ID = 1) then meet.EVENT_HOST end) AS MTNG_HOST
,(case when (tType.R_TYPE_ID = 2) then (case when (find_in_set('PIGO',group_concat(distinct att.PIGO_STATUS separator ',')) > 0) then 1 else 0 end) end) AS PIGO_STAT
,trans.TRANSACTIONAL_AMOUNT AS TRANS_AMT
,trans.R_CURRENCY_ISO_CD AS TRANS_CURR
,'Approved' AS PROCS_STAT
,tHist.CREATED_DATETIME AS OVERALL_PROJ_APPRV_DT
,concat(projectOwner.FIRST_NAME,concat(' ',projectOwner.LAST_NAME)) AS APPRV_INTR_MGR_NAME
,'' AS APPRV_INTR_MGR_DOM
,'' AS APPRV_SPVSR_ID
,'' AS APPRV_SPVSR_NAME
,'' AS APPRV_SPVSR_DT
,(select MAPP_USER.NETWORK_ID from MAPP_USER where (MAPP_USER.GUID = mktGroupApprvlDtls.REVIEWED_BY)) AS APPRV_MKT_FCPA_RVWR_ID
,mktGroupApprvlDtls.REVIEWED_BY AS APPRV_MKT_FCPA_RVWR_NAME
,(select MAPP_USER.NETWORK_ID from MAPP_USER where (MAPP_USER.GUID = lglGroupApprvlDtls.REVIEWED_BY)) AS APPRV_LGL_ID
,lglGroupApprvlDtls.REVIEWED_BY AS APPRV_LGL_NAME
,lglGroupApprvlDtls.REVIEWED_ON AS APPRV_LGL_DT
,(select MAPP_USER.NETWORK_ID from MAPP_USER where (MAPP_USER.GUID = pmgrGroupApprvlDtls.REVIEWED_BY)) AS APPRV_PRNC_MGR_ID
,pmgrGroupApprvlDtls.REVIEWED_BY AS APPRV_PRNC_MGR_NAME
,projectOwner.BUSINESS_UNIT AS RQST_BU
,trans.CREATED_DATETIME AS DT_CRT
,projectOwner.NETWORK_ID AS OWNR
,concat(projectOwner.FIRST_NAME,concat(' ',projectOwner.LAST_NAME)) AS OWNR_NAME
,initiator.NETWORK_ID AS USR_LAN_ID
,concat(initiator.FIRST_NAME,concat(' ',initiator.LAST_NAME)) AS USR_LAN_NAME
,(case when (tType.R_TYPE_ID = 2) then (case when ((select count(att2.ID) 
	from MAPP_ATTENDEES att2 where ((find_in_set(att2.ID,group_concat(att.ID separator ',')) > 0) 
		and (att2.GO_STATUS = 'GO') and (att2.R_ATTENDEE_TYPE_ID = 200))) > 0) then 1 else 0 end) end) AS CNSLT_GO_INTRCT
,(select dtlsOne.REVIEWED_ON 
	from MAPP_APPROVAL_DETAILS dtlsOne 
	where ((dtlsOne.HISTORY_ID = histOne.ID) 
	and (dtlsOne.ID = (
		select max(MAPP_APPROVAL_DETAILS.ID) AS ID 
		from MAPP_APPROVAL_DETAILS 
		where ((MAPP_APPROVAL_DETAILS.HISTORY_ID = histOne.ID) 
		and (MAPP_APPROVAL_DETAILS.REVIEWED_ON is not null)))))) AS RND_ONE_SUBM_DT
,(select concat(usrApprvlDtls.FIRST_NAME,concat(' ',usrApprvlDtls.LAST_NAME)) 
	from (MAPP_APPROVAL_DETAILS subDeets1 join MAPP_USER usrApprvlDtls on((usrApprvlDtls.GUID = subDeets1.REVIEWED_BY))) 
	where (subDeets1.ID = (select max(apprvlDeets.ID) AS ID 
from MAPP_APPROVAL_DETAILS apprvlDeets where (apprvlDeets.HISTORY_ID = histOne.ID)))) AS LATEST_APPRV_BY_NAME
,(select MAPP_APPROVAL_DETAILS.REVIEWED_ON 
	from MAPP_APPROVAL_DETAILS 
	where (MAPP_APPROVAL_DETAILS.ID = (select 
		max(apprvlDeets.ID) AS ID 
		from MAPP_APPROVAL_DETAILS apprvlDeets 
		where (apprvlDeets.HISTORY_ID = histOne.ID)))) AS LATEST_APPRV_BY_DDT
,(case when (tType.R_TYPE_ID in (6,7)) then (case when (ef.CHARITY_NUMBER is not null) then 1 else 0 end) end) AS IS_CHTY_RGSTR
, coalesce(case when (tType.R_TYPE_ID = 2) then  
		(SELECT SUM(TRANSACTIONAL_AMT_USD) FROM MAPP_TRANSACTION
		WHERE ID IN (
			SELECT transaction_id FROM MAPP_EXTERNAL_FUNDING WHERE vendor_id = ef.VENDOR_ID
		))
END, 0.00) AS INCDTL_EXP_AMT
, '' AS DNL_RSN
,trans.EXTERNAL_ID AS PARNT_FCPA_PROJ_ID
,(case when (tType.R_TYPE_ID = 2) then 
	(select count(att3.ID) 
	from MAPP_ATTENDEES att3 
	where ((find_in_set(att3.ID,group_concat(att.ID separator ',')) > 0) 
	and (att3.R_ATTENDEE_TYPE_ID = 200))) end) AS MULT_CONS_GRID_CNT
,(case when (tType.R_TYPE_ID = 2) then (case when (trans.IS_ENTITY_BASED = 1) then 'Entity' when (trans.IS_ENTITY_BASED = 0) then 'Individual' else '' end) end) AS PFE_SUPL_TYP
,case 
	when (tType.R_TYPE_ID = 6) then 'Charitable Donation' 
	when (tType.R_TYPE_ID = 7) then 'Business Donation' 
	else '' 
end AS TYP_OF_DNT_MAPP
,(case when (tType.R_TYPE_ID in (6,7)) then (case when (ef.DIRECT_OR_THIRD_PARTY = 1) then recip.RECIP_NAME else '' end) else '' end) AS RCPNT
,(case when (tType.R_TYPE_ID in (6,7)) then case when ef.DIRECT_OR_THIRD_PARTY = 1 then 'Yes' else 'No' END else 'No' end) AS PD_THRU_TPV
,(case when (projectOwner.COUNTRY = 'VE') then 'E1' else 'SAP' end) AS PFE_ERP
/* 
WBS may change, if so, then we need to wrap ABOVEMARKET in a case when ttype = 1 then ... else '' end statement
CHANGED on 7/3
*/
,case 
	when (tType.R_TYPE_ID = 1) then
	(case when meet.IS_PART_OF_REGIONAL_EVENT then 'LATAM' else '' end)
	else ''
end AS ABOVEMARKET
,contrctRegion.NAME AS ORIGINATINGCOUNTRY
,(case when isnull(ef.VENDOR_ID) then 'Vendor not available' end) AS VNDR_NT_AVAILR 
from (((((((((((((((((((((((((((((
MAPP_TRANSACTION trans 
join MAPP_TYPE tType 
	on((tType.TRANSACTION_ID = trans.ID))) 
join MAPP_TRANSACTION_HISTORY tHist 
	on(((tHist.TRANSACTION_ID = trans.ID) 
	and (tHist.ACTION_ID = 920) 
	and (tHist.CREATED_DATETIME >= date_format((curdate() - interval 1 month),'%Y-%m-01 00:00:00')) 
	and (tHist.CREATED_DATETIME <= date_format(last_day((curdate() - interval 1 month)),'%Y-%m-%d 23:59:59'))))) 
join MAPP_RATIONALE rat 
	on rat.TRANSACTION_ID = trans.ID AND tType.R_TYPE_ID = rat.R_TYPE_ID) 
join MAPP_R_COUNTRY tCtry 
	on((tCtry.COUNTRY_CD = trans.R_COUNTRY_CD))) 
join MAPP_R_DATA subStatus 
	on((subStatus.ID = trans.R_SUB_STATUS_ID))) 
left join MAPP_CONSULTANCY conscy 
	on((conscy.TRANSACTION_ID = trans.ID))) 
left join MAPP_L_CON_SERVICE conSrvc 
	on((conSrvc.CONSULTANCY_ID = conscy.ID)))
LEFT JOIN MAPP_R_DATA serviceData ON serviceData.ID = conSrvc.SERVICE_ID
left join MAPP_MEETING meet 
	on((meet.TRANSACTION_ID = trans.ID))) 
left join MAPP_USER projectOwner 
	on((projectOwner.ID = (case when isnull(trans.ON_BEHALF_OF_USER_ID) then trans.INITIATING_USER_ID else trans.ON_BEHALF_OF_USER_ID end)))) 
left join MAPP_USER initiator 
	on initiator.ID = trans.INITIATING_USER_ID 
left join MAPP_R_COUNTRY usrCntry 
	on((usrCntry.COUNTRY_CD = projectOwner.COUNTRY)))
left join MAPP_R_REGION usrCntryRgn 
	on((usrCntryRgn.ID = usrCntry.R_REGION_ID))) 
left join MAPP_EXTERNAL_FUNDING ef 
	on((ef.TRANSACTION_ID = trans.ID))) 
LEFT JOIN MAPP_R_COUNTRY contrctRegion
	ON contrctRegion.COUNTRY_CD = ef.CONTRACTING_REGION
left join MAPP_VENDOR vend 
	on((vend.ID = ef.VENDOR_ID))) 
left join MAPP_MEETING_LOGISTICS meetLog 
	on((meetLog.TRANSACTION_ID = trans.ID))) 
left join MAPP_L_BR_TOPICS brMeet 
	on((brMeet.RATIONALE_ID = rat.ID))) 
left join MAPP_R_DATA orgType 
	on((orgType.ID = trans.ORG_TYPE))) 
left join MAPP_R_DATA brMeetData 
	on((brMeetData.ID = brMeet.TOPICS_ID))) 
left join MAPP_R_DATA efContrType 
	on((efContrType.ID = ef.R_CONTRIBUTION_TYPE))) 
left join MAPP_REVIEW rvw 
	on((rvw.TRANSACTION_ID = trans.ID))) 
left join MAPP_REVIEW_ATTENDEES revAtt 
	on(((revAtt.REVIEW_ID = rvw.ID) 
	and (revAtt.IS_REJECTED = 0)))) 
left join MAPP_R_RECIPIENTS recip 
	on((recip.ID = ef.R_RECIPIENT_ID))) 
left join MAPP_REVIEW_RBCS rvwRBCs 
	on(((rvwRBCs.REVIEW_ID = rvw.ID) 
	and (rvwRBCs.IS_REJECTED = 0)))) 
left join MAPP_EXTERNAL_RBCS rbcs 
	on((rbcs.ID = rvwRBCs.RBC_ID))) 
left join MAPP_ATTENDEES att 
	on((att.ID = revAtt.ATTENDEE_ID))) 
left join MAPP_APPROVAL_HISTORY histOne 
	on((histOne.TRANSACTION_ID = trans.ID))) 
join (
	select MAPP_APPROVAL_HISTORY.TRANSACTION_ID AS TRANSACTION_ID,max(MAPP_APPROVAL_HISTORY.APPROVAL_CYCLE) AS APPRVL_CYCLE 
	from MAPP_APPROVAL_HISTORY 
	group by MAPP_APPROVAL_HISTORY.TRANSACTION_ID
	) histTwo 
	on(((histOne.TRANSACTION_ID = histTwo.TRANSACTION_ID) and (histOne.APPROVAL_CYCLE = histTwo.APPRVL_CYCLE)))) 
left join MAPP_APPROVAL_DETAILS mktGroupApprvlDtls 
	on((mktGroupApprvlDtls.ID = (
		select max(MAPP_APPROVAL_DETAILS.ID) AS ID from MAPP_APPROVAL_DETAILS where ((MAPP_APPROVAL_DETAILS.HISTORY_ID = histOne.ID) and (MAPP_APPROVAL_DETAILS.GROUP_NAME = 'FCPA Market Reviewer')))))) 
left join MAPP_APPROVAL_DETAILS lglGroupApprvlDtls 
	on((lglGroupApprvlDtls.ID = (
		select max(MAPP_APPROVAL_DETAILS.ID) AS ID 
		from MAPP_APPROVAL_DETAILS 
		where ((MAPP_APPROVAL_DETAILS.HISTORY_ID = histOne.ID) 
		and (MAPP_APPROVAL_DETAILS.GROUP_NAME = 'FCPA Legal')))))) 
left join MAPP_APPROVAL_DETAILS pmgrGroupApprvlDtls 
	on((pmgrGroupApprvlDtls.ID = (
		select max(MAPP_APPROVAL_DETAILS.ID) AS ID 
		from MAPP_APPROVAL_DETAILS 
		where ((MAPP_APPROVAL_DETAILS.HISTORY_ID = histOne.ID) 
		and (MAPP_APPROVAL_DETAILS.GROUP_NAME = 'FCPA Principal Manager/Delegate')))))) 
group by tType.ID ;
