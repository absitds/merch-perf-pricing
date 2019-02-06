#!/bin/ksh
#------------------------------------------------------------------------------#
# File          :  dw_aaai_008_lod_tpt_time.ksh
# Desc          :  Time export for APIC
# Version       :
# Date          :
# Time          :
# WhatString    :
#------------------------------------------------------------------------------#
# Modification  :  Auth  : Aris Fajardo
#               :  Date  : 04/02/2018
#               :  Change:
#               :  Desc  : Initial Version
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
function PrepRptDate
#------------------------------------------------------------------------------#
{
echo -e "\n***START :  PrepRptDate - `date`***"

rm -f ${dateCtlFile}

bteq <<!
.SET SESSIONS 1
.LOGON ${USERID},${PASSWORD};
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.EXPORT REPORT FILE=${dateCtlFile};

SELECT  CASE
            WHEN weekday_nbr IN (6,7) THEN d_date - weekday_nbr + 18
            ELSE d_date - weekday_nbr + 11
        END (FORMAT 'YYYY-MM-DD') (CHAR(10)) (TITLE '')
FROM    ${DWH_DSS_DB}.lu_day_merge
WHERE   d_date = DATE '${LotIdDate}';

.EXPORT RESET;

.LOGOFF;
.QUIT 0;
!

Res=$?
if [ ${Res} != 0 ]; then
    echo -e "ERROR:  Function PrepRptDate failed"
    exit ${Res}
fi

echo -e "\n*** END : PrepRptDate - `date` ***\n"
}


#------------------------------------------------------------------------------#
function PrepData
#------------------------------------------------------------------------------#
{
echo -e "\n***START :  PrepData - `date`***"

bteq <<!
.SET SESSIONS 1
.LOGON ${USERID},${PASSWORD};
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all;

CREATE MULTISET TABLE ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      genrt_cic_id DECIMAL(12,0),
      corp_item_cd DECIMAL(8,0),
      group_id SMALLINT,
      corp INTEGER,
      division INTEGER,
      wds_division CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      facility CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      rog_id INTEGER,
      rog CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      offer_number CHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_num CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_sub_acnt CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      cost_area SMALLINT,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      calendar_date DATE FORMAT 'YY/MM/DD',
      allow_type CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4),
      perform_1 CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( corp_item_cd ,dst_cntr );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all
(       genrt_cic_id
,       corp_item_cd
,       group_id
,       corp
,       division
,       wds_division
,       facility
,       rog_id
,       rog
,       offer_number
,       vend_num
,       vend_sub_acnt
,       cost_area
,       arrival_from_date
,       arrival_to_date
,       calendar_date
,       allow_type
,       allow_amt
,       amt_in_cost
,       perform_1
,       dst_cntr
)
SELECT  d.genrt_cic_id
,       cic.corp_item_cd
,       cic.group_id
,       h.corporation_id    AS corp
,       rog.division_id     AS division
,       h.whse_division_id  AS wds_division
,       d.whse_cd AS facility
,       rog.rog_id
,       rog.rog_cd          AS rog
,       h.vendor_offer_cd AS offer_number
,       h.vend_nbr  AS vend_num
,       h.vend_sub_acct_nbr AS vend_sub_acnt
,       h.cost_area_id AS cost_area
,       h.arrival_start_dt  AS arrival_from_date
,       h.arrival_end_dt  AS arrival_to_date
,       DATE '${rptDate}' AS calendar_date
,       d.allow_type_cd AS allow_type
,       d.allow_amt     AS allow_amt
,       CASE
            WHEN d.allow_type_cd = 'C' THEN allow_amt
            ELSE 0
        END AS amt_in_cost
,       d.performance_1_cd  AS perform_1
,       COALESCE(whs.dst_cntr_cd, d.whse_cd) AS dst_cntr
FROM    ${DWH_DSS_DB}.allow_hdr  h
INNER   JOIN ${DWH_DSS_DB}.allow_dtl  d
ON      h.corporation_id    = d.corporation_id
AND     h.whse_division_id  = d.whse_division_id
AND     h.resp_division_id  = d.resp_division_id
AND     h.vend_nbr          = d.vend_nbr
AND     h.vend_sub_acct_nbr = d.vend_sub_acct_nbr
AND     h.wims_sub_vend_nbr = d.wims_sub_vend_nbr
AND     h.allow_log_nbr     = d.allow_log_nbr
AND     h.cost_area_id      = d.cost_area_id
INNER   JOIN ${DWH_DSS_DB}.lu_cic cic
ON      cic.genrt_cic_id = d.genrt_cic_id
INNER   JOIN ${DWH_DSS_DB}.vendor_item vi
ON      vi.corporation_id = d.corporation_id
AND     vi.whse_cd = d.whse_cd
AND     vi.corp_item_cd = cic.corp_item_cd
INNER   JOIN ${DWH_DSS_DB}.whse_item_rog wir
ON      wir.division_id = vi.division_id
AND     wir.whse_cd = vi.whse_cd
AND     wir.genrt_cic_id = d.genrt_cic_id
INNER   JOIN ${DWH_DSS_DB}.lu_rog rog
ON      rog.rog_id = wir.rog_id
LEFT    OUTER JOIN ${DWH_DSS_DB}.lu_whse whs
ON      whs.whse_cd = d.whse_cd
WHERE   DATE '${rptDate}' BETWEEN h.arrival_start_dt AND h.arrival_end_dt
AND     d.active_cd <> 'I'
AND     d.allow_amt > 0.01
AND     d.reason_cd = '0'
AND     d.allow_type_cd <> 'A'
AND     d.performance_1_cd <> '30'
AND     cic.display_ind <> 'Y'
AND     vi.item_status_cd NOT IN ('D', 'X')
AND     cic.group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37,38,39,40,42,43,44,45,46,47,48,73,74,96,97);
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATS ON ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all INDEX (dst_cntr, corp_item_cd);
COLLECT STATS ON ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all COLUMN (corp, division, vend_num, vend_sub_acnt, cost_area);
COLLECT STATS ON ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all COLUMN (genrt_cic_id, vend_num, vend_sub_acnt);
COLLECT STATS ON ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all COLUMN (genrt_cic_id, rog_id);
COLLECT STATS ON ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all COLUMN (offer_number, division);

DROP TABLE ${DWH_STAGE_DB}.t_pe_alw_final;
CREATE MULTISET TABLE ${DWH_STAGE_DB}.t_pe_alw_final ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      group_id SMALLINT,
      corp_item_cd DECIMAL(8,0),
      corp INTEGER,
      facility CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_num CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_sub_acnt CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      cost_area SMALLINT,
      rog CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      price_area_id INTEGER,
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4),
      offer_number CHAR(8) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( corp_item_cd ,facility );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_alw_final
(       group_id
,       corp_item_cd
,       corp
,       facility
,       vend_num
,       vend_sub_acnt
,       cost_area
,       rog
,       price_area_id
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
,       offer_number
)
SELECT  alw.group_id
,       alw.corp_item_cd
,       alw.corp
,       alw.facility
,       alw.vend_num
,       alw.vend_sub_acnt
,       alw.cost_area
,       alw.rog
,       spa.price_area_id
,       CASE
            WHEN alw.allow_type = 'T' AND alw.perform_1 = '20' THEN 'R'
            ELSE alw.allow_type
        END AS alw_typ
,       alw.arrival_from_date
,       alw.arrival_to_date
,       alw.allow_amt
,       alw.amt_in_cost
,       alw.offer_number
FROM    ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all alw
INNER   JOIN ${DWH_DSS_DB}.vend_cost_area_store vca
ON      vca.corporation_id    = alw.corp
AND     vca.division_id       = alw.division
AND     vca.vend_nbr          = alw.vend_num
AND     vca.vend_sub_acct_nbr = alw.vend_sub_acnt
AND     vca.cost_area_id      = alw.cost_area
AND     vca.hold_status_ind <> 'H'
INNER   JOIN ${DWH_DSS_DB}.dsd_authorization dsd
ON      dsd.genrt_cic_id = alw.genrt_cic_id
AND     dsd.store_id = vca.store_id
AND     dsd.vend_nbr = alw.vend_num
AND     dsd.vend_sub_acct_nbr = alw.vend_sub_acnt
AND     alw.calendar_date BETWEEN dsd.auth_start_dt AND dsd.auth_end_dt
INNER   JOIN ${DWH_DSS_DB}.cic_upc_rog urx
ON      urx.rog_id = alw.rog_id
AND     urx.genrt_cic_id = alw.genrt_cic_id
AND     alw.calendar_date BETWEEN urx.first_eff_dt AND urx.last_eff_dt
INNER   JOIN ${DWH_DSS_DB}.store_price_area spa
ON      spa.rog_id = urx.rog_id
AND     spa.loc_retail_sect_id = urx.loc_retail_sect_id
AND     spa.store_id = vca.store_id
AND     alw.calendar_date BETWEEN spa.first_eff_dt AND spa.last_eff_dt
LEFT    OUTER JOIN ${DWH_DSS_DB}.nopa_allow_hdr nah
ON      nah.vendor_offer_cd = alw.offer_number
AND     nah.deal_division_cd = alw.division
AND     nah.current_record_ind = 'Y'
WHERE   alw.cost_area > 0
GROUP   BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

UNION

SELECT  alw.group_id
,       alw.corp_item_cd
,       alw.corp
,       alw.facility
,       alw.vend_num
,       alw.vend_sub_acnt
,       alw.cost_area
,       alw.rog
,       spa.price_area_id
,       CASE
            WHEN alw.allow_type = 'T' AND alw.perform_1 = '20' THEN 'R'
            ELSE alw.allow_type
        END AS alw_typ
,       alw.arrival_from_date
,       alw.arrival_to_date
,       alw.allow_amt
,       alw.amt_in_cost
,       alw.offer_number
FROM    ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all alw
INNER   JOIN ${DWH_DSS_DB}.cic_upc_rog urx
ON      urx.rog_id = alw.rog_id
AND     urx.genrt_cic_id = alw.genrt_cic_id
AND     alw.calendar_date BETWEEN urx.first_eff_dt AND urx.last_eff_dt
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.rog_id = urx.rog_id
AND     str.closed_dt = DATE '9999-12-31'
AND     str.corporation_id = 1
INNER   JOIN ${DWH_DSS_DB}.store_price_area spa
ON      spa.rog_id = urx.rog_id
AND     spa.loc_retail_sect_id = urx.loc_retail_sect_id
AND     spa.store_id = str.store_id
AND     alw.calendar_date BETWEEN spa.first_eff_dt AND spa.last_eff_dt
LEFT    OUTER JOIN ${DWH_DSS_DB}.nopa_allow_hdr nah
ON      nah.vendor_offer_cd = alw.offer_number
AND     nah.deal_division_cd = alw.division
AND     nah.current_record_ind = 'Y'
WHERE   alw.cost_area = 0
AND     (nah.vendor_offer_cd IS NOT NULL
OR      (alw.wds_division <> '65' AND nah.vendor_offer_cd IS NULL))
GROUP   BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_alw_final INDEX (facility, corp_item_cd);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_alw_final COLUMN (corp, facility, corp_item_cd, rog, price_area_id, vend_num, vend_sub_acnt, cost_area);

DROP TABLE ${DWH_STAGE_DB}.t_pe_cpn_adplan_all;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_cpn_adplan_all ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_id INTEGER,
      upc_id DECIMAL(14,0),
      offer_id DECIMAL(13,0),
      OFFER_TYPE_CD CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_cpn_adplan_all
(       division_id
,       rog_id
,       rog_cd
,       store_id
,       upc_id
,       offer_id
,       offer_type_cd
,       coupon_amt
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
)
SELECT  str.division_id
,       str.rog_id
,       str.rog_cd
,       sipp.store_id
,       sipp.upc_id
,       sipp.offer_id
,       ofr.offer_type_cd
,       ofr.coupon_amt
,       sipp.promo_method_cd
,       sipp.promo_min_purch_qty
,       sipp.promo_lim_qty
,       sipp.promo_prc
,       sipp.promo_prc_fctr
,       sipp.first_eff_dt
,       sipp.last_eff_dt
FROM    ${DWH_DSS_DB}.lu_offer ofr
INNER   JOIN ${DWH_DSS_DB}.store_item_promo_prc sipp
ON      sipp.offer_id = ofr.offer_id
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.store_id = sipp.store_id
AND     str.closed_dt > CURRENT_DATE
AND     str.corporation_id = 1
WHERE   ofr.offer_type_cd IN ('ECP', 'PCP', 'AD', 'LTS')
AND     DATE '${rptDate}' BETWEEN sipp.first_eff_dt AND sipp.last_eff_dt;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_cpn_adplan_all INDEX (store_id, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_cpn_adplan_all COLUMN (rog_cd, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_cpn_adplan_all COLUMN (offer_type_cd);

DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_reg_rtl;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_pending_reg_rtl ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_id INTEGER,
      upc_id DECIMAL(14,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0))
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_pending_reg_rtl
(       division_id
,       rog_id
,       rog_cd
,       store_id
,       upc_id
,       first_eff_dt
,       reg_rtl_prc
,       reg_rtl_prc_fctr
)
SELECT  str.division_id
,       str.rog_id
,       str.rog_cd
,       reg.store_id
,       reg.upc_id
,       reg.first_eff_dt
,       reg.reg_rtl_prc
,       reg.reg_rtl_prc_fctr
FROM    ${DWH_DSS_DB}.store_item_reg_rtl_futr reg
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.store_id = reg.store_id
AND     str.closed_dt > CURRENT_DATE
AND     str.corporation_id = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY reg.store_id, reg.upc_id ORDER BY reg.first_eff_dt ASC, reg.last_eff_dt ASC) = 1;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_pending_reg_rtl INDEX (store_id, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_pending_reg_rtl COLUMN (rog_cd, upc_id);

DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_dsd_cost;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_pending_dsd_cost ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      corporation_id VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      division_id INTEGER,
      CORP_ITEM_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      COST_AREA_ID SMALLINT,
      date_eff DATE FORMAT 'YY/MM/DD',
      cost_vend DECIMAL(9,4))
PRIMARY INDEX ( division_id ,CORP_ITEM_CD );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_pending_dsd_cost
(       corporation_id
,       division_id
,       corp_item_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       date_eff
,       cost_vend
)
SELECT  corporation_cd  AS corporation_id
,       division_cd (INTEGER)   AS division_id
,       corp_item_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       start_eff_dt    AS date_eff
,       vend_cst        AS cost_vend
FROM    ${DWH_DSS_DB}.dsd_cost_pending
WHERE   date_eff > CURRENT_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY corporation_cd, division_cd, corp_item_cd, vend_nbr, vend_sub_acct_nbr, cost_area_id ORDER BY start_eff_dt ASC, vend_cst ASC) = 1;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_pending_dsd_cost INDEX (division_id, corp_item_cd);

DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_whse_cost;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_pending_whse_cost ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      CORP_ITEM_CD INTEGER,
      date_eff DATE FORMAT 'YY/MM/DD',
      cost_vend DECIMAL(9,4))
PRIMARY INDEX ( dst_cntr ,CORP_ITEM_CD );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_pending_whse_cost
(       dst_cntr
,       corp_item_cd
,       date_eff
,       cost_vend
)
SELECT  dst_cntr_cd     AS dst_cntr
,       corp_item_cd
,       start_eff_dt    AS date_eff
,       vend_cst        AS cost_vend
FROM    ${DWH_DSS_DB}.whse_cost_pending
QUALIFY ROW_NUMBER() OVER (PARTITION BY dst_cntr_cd, corp_item_cd ORDER BY start_eff_dt ASC, vend_cst ASC) = 1;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_pending_whse_cost INDEX (dst_cntr, corp_item_cd);

DROP TABLE ${DWH_STAGE_DB}.t_pe_hist_reg_rtl;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_hist_reg_rtl ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      store_id INTEGER,
      upc_id DECIMAL(14,0),
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0))
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_hist_reg_rtl
(       division_id
,       rog_id
,       rog_cd
,       store_id
,       upc_id
,       last_eff_dt
,       reg_rtl_prc
,       reg_rtl_prc_fctr
)
SELECT  str.division_id
,       str.rog_id
,       str.rog_cd
,       reg.store_id
,       reg.upc_id
,       reg.last_eff_dt
,       reg.reg_rtl_prc
,       reg.reg_rtl_prc_fctr
FROM    ${DWH_DSS_DB}.store_item_reg_rtl reg
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.store_id = reg.store_id
AND     str.closed_dt > CURRENT_DATE
AND     str.corporation_id = 1
WHERE   reg.last_eff_dt < CURRENT_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY reg.store_id, reg.upc_id ORDER BY reg.last_eff_dt DESC, reg.first_eff_dt DESC) = 1;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_hist_reg_rtl INDEX (store_id, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_hist_reg_rtl COLUMN (rog_cd, upc_id);

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1;
CREATE MULTISET TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      store_id INTEGER,
      genrt_cic_id DECIMAL(12,0),
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_whse_qty DECIMAL(7,2),
      pack_retail_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT)
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1
(       division_id
,       store_id
,       genrt_cic_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_whse_qty
,       pack_retail_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
)
SELECT  str.division_id
,       str.store_id
,       cic.genrt_cic_id
,       cic.corp_item_cd
,       cic.cic_dsc
,       cic.group_id
,       cic.category_id
,       cic.vend_conv_fctr
,       cic.pack_whse_qty
,       urx.pack_retail_qty
,       urx.pack_dsc
,       urx.size_dsc
,       wds.dst_cntr_cd AS dst_cntr
,       vcc.vend_cst AS cic_vend_cst
,       1 AS corporation_id
,       wds.whse_cd
,       wds.status_dst_cd   AS status_dst
,       vcc.ib_cst AS cic_ib_cst
,       cic.item_type_cd
,       str.rog_id
,       str.rog_cd
,       wir.retail_status_cd
,       urx.loc_common_retail_cd
,       urx.upc_id
,       urx.snstv_tier_cd
,       urx.div_promo_grp_cd
,       wds.vend_nbr
,       wds.vend_sub_acct_nbr
,       urx.loc_retail_sect_id
,       wds.buyer_nbr    -- wds.buyer_nbr
,       urx.unit_prc_tbl_nbr
FROM    ${DWH_DSS_DB}.whse_cost_src wds
INNER   JOIN ${DWH_DSS_DB}.lu_cic cic
ON      cic.corp_item_cd = wds.corp_item_cd
AND     cic.non_merged_cic_ind = 'Y'
AND     cic.corporation_id = 1
AND     cic.display_ind <> 'Y'
INNER   JOIN ${DWH_DSS_DB}.whse_item_rog wir
ON      wir.whse_division_id = wds.division_cd
AND     wir.whse_cd = wds.whse_cd
AND     wir.genrt_cic_id = cic.genrt_cic_id
AND     wir.retail_status_cd <> 'D'
INNER   JOIN ${DWH_DSS_DB}.dsd_cost_src vcc
ON      vcc.division_cd (INTEGER) = wir.division_id
AND     vcc.corp_item_cd = wds.corp_item_cd
AND     vcc.vend_nbr = wds.vend_nbr
AND     vcc.vend_sub_acct_nbr = wds.vend_sub_acct_nbr
INNER   JOIN ${DWH_DSS_DB}.cic_upc_rog urx
ON      urx.rog_id = wir.rog_id
AND     urx.genrt_cic_id = wir.genrt_cic_id
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.rog_id = urx.rog_id
AND     str.corporation_id = 1
AND     str.closed_dt > CURRENT_DATE
AND     str.division_id NOT IN (45, 58)
INNER   JOIN ${DWH_DSS_DB}.dsd_authorization dsd
ON      dsd.genrt_cic_id = cic.genrt_cic_id
AND     dsd.store_id = str.store_id
AND     dsd.vend_nbr = vcc.vend_nbr
AND     dsd.vend_sub_acct_nbr = vcc.vend_sub_acct_nbr
AND     DATE '${rptDate}' BETWEEN dsd.auth_start_dt AND dsd.auth_end_dt
WHERE   wds.status_dst_cd NOT IN ('X', 'D')
AND     SUBSTR(wds.whse_cd, 1, 2) = 'DD'
AND     cic.group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37,38,39,40,42,43,44,45,46,47,48,73,74,96,97);
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 INDEX (store_id, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 COLUMN (store_id, loc_retail_sect_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 COLUMN (buyer_nbr);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 COLUMN (store_id, vend_nbr, vend_sub_acct_nbr);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 COLUMN (vend_nbr, corporation_id);

DROP TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_1;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      store_id INTEGER,
      genrt_cic_id DECIMAL(12,0),
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_whse_qty DECIMAL(7,2),
      pack_retail_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT)
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_whse_item_attr_1
(       division_id
,       store_id
,       genrt_cic_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_whse_qty
,       pack_retail_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
)
SELECT  str.division_id
,       str.store_id
,       cic.genrt_cic_id
,       cic.corp_item_cd
,       cic.cic_dsc
,       cic.group_id
,       cic.category_id
,       cic.vend_conv_fctr
,       cic.pack_whse_qty
,       urx.pack_retail_qty
,       urx.pack_dsc
,       urx.size_dsc
,       wds.dst_cntr_cd AS dst_cntr
,       wds.vend_cst AS cic_vend_cst
,       1 AS corporation_id
,       wds.whse_cd
,       wds.status_dst_cd   AS status_dst
,       wds.ib_cst AS cic_ib_cst
,       cic.item_type_cd
,       rog.rog_id
,       rog.rog_cd
,       wir.retail_status_cd
,       urx.loc_common_retail_cd
,       urx.upc_id
,       urx.snstv_tier_cd
,       urx.div_promo_grp_cd
,       wds.vend_nbr
,       wds.vend_sub_acct_nbr
,       urx.loc_retail_sect_id
,       wds.buyer_nbr
,       urx.unit_prc_tbl_nbr
FROM    ${DWH_DSS_DB}.whse_cost_src wds
INNER   JOIN ${DWH_DSS_DB}.lu_cic cic
ON      cic.corp_item_cd = wds.corp_item_cd
AND     cic.non_merged_cic_ind = 'Y'
AND     cic.corporation_id = 1
AND     cic.display_ind <> 'Y'
INNER   JOIN ${DWH_DSS_DB}.whse_item_rog wir
ON      wir.whse_division_id = wds.division_cd
AND     wir.whse_cd = wds.whse_cd
AND     wir.genrt_cic_id = cic.genrt_cic_id
AND     wir.retail_status_cd <> 'D'
INNER   JOIN ${DWH_DSS_DB}.cic_upc_rog urx
ON      urx.rog_id = wir.rog_id
AND     urx.genrt_cic_id = wir.genrt_cic_id
INNER   JOIN ${DWH_DSS_DB}.lu_rog rog
ON      rog.rog_id = urx.rog_id
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.rog_id = rog.rog_id
AND     str.closed_dt > CURRENT_DATE
AND     str.corporation_id = 1
AND     str.division_id NOT IN (45, 58)
WHERE   wds.status_dst_cd NOT IN ('X', 'D')
AND     SUBSTR(wds.whse_cd, 1, 2) <> 'DD'
AND     cic.group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37,38,39,40,42,43,44,45,46,47,48,73,74,96,97);
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 INDEX (store_id, upc_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 COLUMN (store_id, loc_retail_sect_id);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 COLUMN (buyer_nbr);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 COLUMN (store_id, vend_nbr, vend_sub_acct_nbr);
COLLECT STATISTICS ON ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 COLUMN (vend_nbr, corporation_id);

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_2;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_2 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      store_id INTEGER,
      genrt_cic_id DECIMAL(12,0),
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_whse_qty DECIMAL(7,2),
      pack_retail_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT,
      price_area_id INTEGER,
      cost_area_id SMALLINT,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nm CHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0),
      offer_id DECIMAL(13,0),
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_first_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_reg_rtl_prc DECIMAL(7,2),
      prtl_reg_rtl_prc_fctr DECIMAL(2,0),
      adplan_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      up_mult_factor DECIMAL(7,3),
      up_label_unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      pending_cost DECIMAL(9,4),
      pending_cost_date DATE FORMAT 'YY/MM/DD',
      hist_reg_rtl_prc DECIMAL(7,2),
      hist_reg_rtl_prc_fctr DECIMAL(2,0),
      hist_last_eff_dt DATE FORMAT 'YY/MM/DD',
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4))
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_item_attr_2
(       division_id
,       store_id
,       genrt_cic_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_whse_qty
,       pack_retail_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
)
SELECT  tmp.division_id
,       tmp.store_id
,       tmp.genrt_cic_id
,       tmp.corp_item_cd
,       tmp.cic_dsc
,       tmp.group_id
,       tmp.category_id
,       tmp.vend_conv_fctr
,       tmp.pack_whse_qty
,       tmp.pack_retail_qty
,       tmp.pack_dsc
,       tmp.size_dsc
,       tmp.dst_cntr
,       tmp.cic_vend_cst
,       tmp.corporation_id
,       tmp.WHSE_CD
,       tmp.status_dst
,       tmp.cic_ib_cst
,       tmp.item_type_cd
,       tmp.rog_id
,       tmp.rog_cd
,       tmp.retail_status_cd
,       tmp.loc_common_retail_cd
,       tmp.upc_id
,       tmp.snstv_tier_cd
,       tmp.DIV_PROMO_GRP_CD
,       tmp.VEND_NBR
,       tmp.VEND_SUB_ACCT_NBR
,       tmp.loc_retail_sect_id
,       tmp.BUYER_NBR
,       tmp.unit_prc_tbl_nbr
,       spa.price_area_id
,       vca.cost_area_id
,       buy.buyer_nm
,       ven.vend_nm
,       reg.reg_rtl_prc
,       reg.reg_rtl_prc_fctr
,       cpn.offer_id
,       cpn.coupon_amt
,       cpn.promo_method_cd
,       cpn.promo_min_purch_qty
,       cpn.promo_lim_qty
,       cpn.promo_prc
,       cpn.promo_prc_fctr
,       cpn.first_eff_dt
,       cpn.last_eff_dt
,       prtl.first_eff_dt AS prtl_first_eff_dt
,       prtl.reg_rtl_prc  AS prtl_reg_rtl_prc
,       prtl.reg_rtl_prc_fctr   AS prtl_reg_rtl_prc_fctr
,       CASE WHEN adp.store_id IS NULL THEN ' ' ELSE 'Y' END AS adplan_flag
,       uni.unit_prc_mult_fctr  AS up_mult_factor
,       uni.unit_prc_label_dsc  AS up_label_unit
,       COALESCE(dcst.cost_vend, wcst.cost_vend) AS pending_cost
,       COALESCE(dcst.date_eff, wcst.date_eff) AS pending_cost_date
,       hrtl.reg_rtl_prc AS hist_reg_rtl_prc
,       hrtl.reg_rtl_prc_fctr AS hist_reg_rtl_prc_fctr
,       hrtl.last_eff_dt AS hist_last_eff_dt
,       alw.alw_typ
,       alw.arrival_from_date
,       alw.arrival_to_date
,       alw.allow_amt
,       alw.amt_in_cost
FROM    ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1 tmp
INNER   JOIN (
        SELECT  spa.rog_id
        ,       rog.rog_cd
        ,       spa.loc_retail_sect_id
        ,       spa.price_area_id
        ,       spa.store_id
        FROM    ${DWH_DSS_DB}.store_price_area spa
        INNER   JOIN ${DWH_DSS_DB}.lu_rog rog
        ON      rog.rog_id = spa.rog_id
        WHERE   spa.last_eff_dt = DATE '9999-12-31'
        AND     NOT (rog.rog_cd = 'SEAS'
        AND     spa.price_area_id = 1)
        AND     NOT (rog.rog_cd = 'AIMT'
        AND     spa.price_area_id IN (13, 14, 18))
        ) spa
ON      spa.loc_retail_sect_id = tmp.loc_retail_sect_id
AND     spa.store_id = tmp.store_id
INNER   JOIN ${DWH_DSS_DB}.vend_cost_area_store vca
ON      vca.vend_nbr = tmp.vend_nbr
AND     vca.vend_sub_acct_nbr = tmp.vend_sub_acct_nbr
AND     vca.store_id = tmp.store_id
AND     vca.hold_status_ind <> 'H'
INNER   JOIN ${DWH_DSS_DB}.lu_buyer buy
ON      buy.buyer_nbr = tmp.buyer_nbr
INNER   JOIN ${DWH_DSS_DB}.lu_vendor ven
ON      ven.vend_nbr = tmp.vend_nbr
AND     ven.corporation_id = tmp.corporation_id
LEFT    OUTER JOIN ${DWH_DSS_DB}.store_item_reg_rtl reg
ON      reg.store_id = tmp.store_id
AND     reg.upc_id   = tmp.upc_id
AND     CURRENT_DATE BETWEEN reg.first_eff_dt AND reg.last_eff_dt
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_cpn_adplan_all cpn
ON      cpn.store_id = tmp.store_id
AND     cpn.upc_id = tmp.upc_id
AND     cpn.offer_type_cd IN ('ECP', 'PCP')
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_pending_reg_rtl prtl
ON      prtl.store_id = tmp.store_id
AND     prtl.upc_id   = tmp.upc_id
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_cpn_adplan_all adp
ON      adp.store_id = tmp.store_id
AND     adp.upc_id = tmp.upc_id
AND     adp.offer_type_cd IN ('AD', 'LTS')
LEFT    OUTER JOIN ${DWH_DSS_DB}.unit_prc_meas uni
ON      uni.unit_prc_id = tmp.unit_prc_tbl_nbr
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_pending_dsd_cost dcst
ON      dcst.corp_item_cd  = tmp.corp_item_cd
AND     dcst.vend_nbr      = tmp.vend_nbr
AND     dcst.vend_sub_acct_nbr = tmp.vend_sub_acct_nbr
AND     dcst.cost_area_id = vca.cost_area_id
AND     dcst.division_id = tmp.division_id
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_pending_whse_cost wcst
ON      wcst.dst_cntr = tmp.dst_cntr
AND     wcst.corp_item_cd = tmp.corp_item_cd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_hist_reg_rtl hrtl
ON      hrtl.store_id = tmp.store_id
AND     hrtl.upc_id   = tmp.upc_id
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_alw_final alw
ON      alw.corp = tmp.corporation_id
AND     alw.facility = tmp.whse_cd
AND     alw.corp_item_cd = tmp.corp_item_cd
AND     alw.rog = tmp.rog_cd
AND     alw.price_area_id = spa.price_area_id
AND     alw.vend_num = tmp.vend_nbr
AND     alw.vend_sub_acnt = tmp.vend_sub_acct_nbr
AND     alw.cost_area = vca.cost_area_id;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_2;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_2 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      store_id INTEGER,
      genrt_cic_id DECIMAL(12,0),
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_whse_qty DECIMAL(7,2),
      pack_retail_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT,
      price_area_id INTEGER,
      cost_area_id SMALLINT,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nm CHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0),
      offer_id DECIMAL(13,0),
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_first_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_reg_rtl_prc DECIMAL(7,2),
      prtl_reg_rtl_prc_fctr DECIMAL(2,0),
      adplan_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      up_mult_factor DECIMAL(7,3),
      up_label_unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      pending_cost DECIMAL(9,4),
      pending_cost_date DATE FORMAT 'YY/MM/DD',
      hist_reg_rtl_prc DECIMAL(7,2),
      hist_reg_rtl_prc_fctr DECIMAL(2,0),
      hist_last_eff_dt DATE FORMAT 'YY/MM/DD',
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4))
PRIMARY INDEX ( store_id ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_whse_item_attr_2
(       division_id
,       store_id
,       genrt_cic_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_whse_qty
,       pack_retail_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
)
SELECT  tmp.division_id
,       tmp.store_id
,       tmp.genrt_cic_id
,       tmp.corp_item_cd
,       tmp.cic_dsc
,       tmp.group_id
,       tmp.category_id
,       tmp.vend_conv_fctr
,       tmp.pack_whse_qty
,       tmp.pack_retail_qty
,       tmp.pack_dsc
,       tmp.size_dsc
,       tmp.dst_cntr
,       tmp.cic_vend_cst
,       tmp.corporation_id
,       tmp.WHSE_CD
,       tmp.status_dst
,       tmp.cic_ib_cst
,       tmp.item_type_cd
,       tmp.rog_id
,       tmp.rog_cd
,       tmp.retail_status_cd
,       tmp.loc_common_retail_cd
,       tmp.upc_id
,       tmp.snstv_tier_cd
,       tmp.DIV_PROMO_GRP_CD
,       tmp.VEND_NBR
,       tmp.VEND_SUB_ACCT_NBR
,       tmp.loc_retail_sect_id
,       tmp.BUYER_NBR
,       tmp.unit_prc_tbl_nbr
,       spa.price_area_id
,       0 (SMALLINT) AS cost_area_id
,       buy.buyer_nm
,       ven.vend_nm
,       reg.reg_rtl_prc
,       reg.reg_rtl_prc_fctr
,       cpn.offer_id
,       cpn.coupon_amt
,       cpn.promo_method_cd
,       cpn.promo_min_purch_qty
,       cpn.promo_lim_qty
,       cpn.promo_prc
,       cpn.promo_prc_fctr
,       cpn.first_eff_dt
,       cpn.last_eff_dt
,       prtl.first_eff_dt AS prtl_first_eff_dt
,       prtl.reg_rtl_prc  AS prtl_reg_rtl_prc
,       prtl.reg_rtl_prc_fctr   AS prtl_reg_rtl_prc_fctr
,       CASE WHEN adp.store_id IS NULL THEN ' ' ELSE 'Y' END AS adplan_flag
,       uni.unit_prc_mult_fctr  AS up_mult_factor
,       uni.unit_prc_label_dsc  AS up_label_unit
,       wcst.cost_vend AS pending_cost
,       wcst.date_eff AS pending_cost_date
,       hrtl.reg_rtl_prc AS hist_reg_rtl_prc
,       hrtl.reg_rtl_prc_fctr AS hist_reg_rtl_prc_fctr
,       hrtl.last_eff_dt AS hist_last_eff_dt
,       alw.alw_typ
,       alw.arrival_from_date
,       alw.arrival_to_date
,       alw.allow_amt
,       alw.amt_in_cost
FROM    ${DWH_STAGE_DB}.t_pe_whse_item_attr_1 tmp
INNER   JOIN (
        SELECT  spa.rog_id
        ,       rog.rog_cd
        ,       spa.loc_retail_sect_id
        ,       spa.price_area_id
        ,       spa.store_id
        FROM    ${DWH_DSS_DB}.store_price_area spa
        INNER   JOIN ${DWH_DSS_DB}.lu_rog rog
        ON      rog.rog_id = spa.rog_id
        WHERE   spa.last_eff_dt = DATE '9999-12-31'
        AND     NOT (rog.rog_cd = 'SEAS'  -- SEAS price area exclude list
        AND     spa.price_area_id = 1)
--        AND     NOT (rog.rog_cd = 'AIMT'  -- AIMT price area exclude list
--        AND     spa.price_area_id IN (13, 14, 18))
        ) spa
ON      spa.loc_retail_sect_id = tmp.loc_retail_sect_id
AND     spa.store_id = tmp.store_id
INNER   JOIN ${DWH_DSS_DB}.lu_buyer buy
ON      buy.buyer_nbr = tmp.buyer_nbr
INNER   JOIN ${DWH_DSS_DB}.lu_vendor ven
ON      ven.vend_nbr = tmp.vend_nbr
AND     ven.corporation_id = tmp.corporation_id
LEFT    OUTER JOIN ${DWH_DSS_DB}.store_item_reg_rtl reg
ON      reg.store_id = tmp.store_id
AND     reg.upc_id   = tmp.upc_id
AND     CURRENT_DATE BETWEEN reg.first_eff_dt AND reg.last_eff_dt
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_cpn_adplan_all cpn
ON      cpn.store_id = tmp.store_id
AND     cpn.upc_id = tmp.upc_id
AND     cpn.offer_type_cd IN ('ECP', 'PCP')
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_pending_reg_rtl prtl
ON      prtl.store_id = tmp.store_id
AND     prtl.upc_id   = tmp.upc_id
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_cpn_adplan_all adp
ON      adp.store_id = tmp.store_id
AND     adp.upc_id = tmp.upc_id
AND     adp.offer_type_cd IN ('AD', 'LTS')
LEFT    OUTER JOIN ${DWH_DSS_DB}.unit_prc_meas uni
ON      uni.unit_prc_id = tmp.unit_prc_tbl_nbr
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_pending_whse_cost wcst
ON      wcst.dst_cntr = tmp.dst_cntr
AND     wcst.corp_item_cd = tmp.corp_item_cd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_hist_reg_rtl hrtl
ON      hrtl.store_id = tmp.store_id
AND     hrtl.upc_id   = tmp.upc_id
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_alw_final alw
ON      alw.corp = tmp.corporation_id
AND     alw.facility = tmp.whse_cd
AND     alw.corp_item_cd = tmp.corp_item_cd
AND     alw.rog = tmp.rog_cd
AND     alw.price_area_id = spa.price_area_id
AND     alw.vend_num = tmp.vend_nbr
AND     alw.vend_sub_acnt = tmp.vend_sub_acct_nbr
AND     alw.cost_area = 0;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      store_id INTEGER,
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_retail_qty DECIMAL(7,2),
      pack_whse_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT,
      price_area_id INTEGER,
      cost_area_id SMALLINT,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nm CHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0),
      offer_id DECIMAL(13,0),
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_first_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_reg_rtl_prc DECIMAL(7,2),
      prtl_reg_rtl_prc_fctr DECIMAL(2,0),
      adplan_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      up_mult_factor DECIMAL(7,3),
      up_label_unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      pending_cost DECIMAL(9,4),
      pending_cost_date DATE FORMAT 'YY/MM/DD',
      hist_reg_rtl_prc DECIMAL(7,2),
      hist_reg_rtl_prc_fctr DECIMAL(2,0),
      hist_last_eff_dt DATE FORMAT 'YY/MM/DD',
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4))
PRIMARY INDEX ( store_id ,corp_item_cd );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
(       division_id
,       store_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
)
SELECT  division_id
,       store_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
FROM    ${DWH_STAGE_DB}.t_pe_dsd_item_attr_2
GROUP   BY division_id
,       store_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost

UNION ALL

SELECT  division_id
,       store_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
FROM    ${DWH_STAGE_DB}.t_pe_whse_item_attr_2
GROUP   BY division_id
,       store_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_4;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_4 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_retail_qty DECIMAL(7,2),
      pack_whse_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT,
      price_area_id INTEGER,
      cost_area_id SMALLINT,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nm CHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0),
      offer_id DECIMAL(13,0),
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_first_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_reg_rtl_prc DECIMAL(7,2),
      prtl_reg_rtl_prc_fctr DECIMAL(2,0),
      adplan_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      up_mult_factor DECIMAL(7,3),
      up_label_unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      pending_cost DECIMAL(9,4),
      pending_cost_date DATE FORMAT 'YY/MM/DD',
      hist_reg_rtl_prc DECIMAL(7,2),
      hist_reg_rtl_prc_fctr DECIMAL(2,0),
      hist_last_eff_dt DATE FORMAT 'YY/MM/DD',
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4))
PRIMARY INDEX ( corp_item_cd ,WHSE_CD );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_4
(       division_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
)
SELECT  division_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
GROUP   BY division_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       whse_cd
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       div_promo_grp_cd
,       vend_nbr
,       vend_sub_acct_nbr
,       loc_retail_sect_id
,       buyer_nbr
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       offer_id
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_5;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_5 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      corp_item_cd DECIMAL(8,0),
      cic_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id SMALLINT,
      category_id SMALLINT,
      vend_conv_fctr SMALLINT,
      pack_retail_qty DECIMAL(7,2),
      pack_whse_qty DECIMAL(7,2),
      pack_dsc CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_cntr VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_vend_cst DECIMAL(9,4),
      corporation_id BYTEINT,
      WHSE_CD VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      status_dst CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      cic_ib_cst DECIMAL(9,4),
      item_type_cd DECIMAL(3,0),
      rog_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      retail_status_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_common_retail_cd DECIMAL(5,0),
      upc_id DECIMAL(13,0),
      snstv_tier_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      loc_retail_sect_id INTEGER,
      BUYER_NBR CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      unit_prc_tbl_nbr SMALLINT,
      price_area_id INTEGER,
      cost_area_id SMALLINT,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nm CHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      reg_rtl_prc DECIMAL(7,2),
      reg_rtl_prc_fctr DECIMAL(2,0),
      COUPON_AMT DECIMAL(8,2),
      promo_method_cd CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_min_purch_qty DECIMAL(2,0),
      promo_lim_qty SMALLINT,
      promo_prc DECIMAL(7,2),
      promo_prc_fctr DECIMAL(2,0),
      first_eff_dt DATE FORMAT 'YY/MM/DD',
      last_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_first_eff_dt DATE FORMAT 'YY/MM/DD',
      prtl_reg_rtl_prc DECIMAL(7,2),
      prtl_reg_rtl_prc_fctr DECIMAL(2,0),
      adplan_flag VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      up_mult_factor DECIMAL(7,3),
      up_label_unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      pending_cost DECIMAL(9,4),
      pending_cost_date DATE FORMAT 'YY/MM/DD',
      hist_reg_rtl_prc DECIMAL(7,2),
      hist_reg_rtl_prc_fctr DECIMAL(2,0),
      hist_last_eff_dt DATE FORMAT 'YY/MM/DD',
      alw_typ VARCHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      arrival_from_date DATE FORMAT 'YY/MM/DD',
      arrival_to_date DATE FORMAT 'YY/MM/DD',
      allow_amt DECIMAL(11,4),
      amt_in_cost DECIMAL(11,4))
PRIMARY INDEX ( corp_item_cd ,WHSE_CD );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_5
(       division_id
,       corp_item_cd
,       cic_dsc
,       group_id
,       category_id
,       vend_conv_fctr
,       pack_retail_qty
,       pack_whse_qty
,       pack_dsc
,       size_dsc
,       dst_cntr
,       cic_vend_cst
,       corporation_id
,       WHSE_CD
,       status_dst
,       cic_ib_cst
,       item_type_cd
,       rog_id
,       rog_cd
,       retail_status_cd
,       loc_common_retail_cd
,       upc_id
,       snstv_tier_cd
,       DIV_PROMO_GRP_CD
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       loc_retail_sect_id
,       BUYER_NBR
,       unit_prc_tbl_nbr
,       price_area_id
,       cost_area_id
,       buyer_nm
,       vend_nm
,       reg_rtl_prc
,       reg_rtl_prc_fctr
,       COUPON_AMT
,       promo_method_cd
,       promo_min_purch_qty
,       promo_lim_qty
,       promo_prc
,       promo_prc_fctr
,       first_eff_dt
,       last_eff_dt
,       prtl_first_eff_dt
,       prtl_reg_rtl_prc
,       prtl_reg_rtl_prc_fctr
,       adplan_flag
,       up_mult_factor
,       up_label_unit
,       pending_cost
,       pending_cost_date
,       hist_reg_rtl_prc
,       hist_reg_rtl_prc_fctr
,       hist_last_eff_dt
,       alw_typ
,       arrival_from_date
,       arrival_to_date
,       allow_amt
,       amt_in_cost
)
SELECT  dt.division_id
,       dt.corp_item_cd
,       dt.cic_dsc
,       dt.group_id
,       dt.category_id
,       dt.vend_conv_fctr
,       dt.pack_retail_qty
,       dt.pack_whse_qty
,       dt.pack_dsc
,       dt.size_dsc
,       dt.dst_cntr
,       dt.cic_vend_cst
,       dt.corporation_id
,       dt.whse_cd
,       dt.status_dst
,       dt.cic_ib_cst
,       dt.item_type_cd
,       dt.rog_id
,       dt.rog_cd
,       dt.retail_status_cd
,       dt.loc_common_retail_cd
,       dt.upc_id
,       dt.snstv_tier_cd
,       dt.div_promo_grp_cd
,       dt.vend_nbr
,       dt.vend_sub_acct_nbr
,       dt.loc_retail_sect_id
,       dt.buyer_nbr
,       dt.unit_prc_tbl_nbr
,       dt.price_area_id
,       dt.cost_area_id
,       dt.buyer_nm
,       dt.vend_nm
,       reg.reg_rtl_prc
,       reg.reg_rtl_prc_fctr
,       promo.COUPON_AMT
,       promo.promo_method_cd
,       promo.promo_min_purch_qty
,       promo.promo_lim_qty
,       promo.promo_prc
,       promo.promo_prc_fctr
,       promo.first_eff_dt
,       promo.last_eff_dt
,       prtl.prtl_first_eff_dt
,       prtl.prtl_reg_rtl_prc
,       prtl.prtl_reg_rtl_prc_fctr
,       dt.adplan_flag
,       dt.up_mult_factor
,       dt.up_label_unit
,       dt.pending_cost
,       dt.pending_cost_date
,       hreg.hist_reg_rtl_prc
,       hreg.hist_reg_rtl_prc_fctr
,       hreg.hist_last_eff_dt
,       dt.alw_typ
,       dt.arrival_from_date
,       dt.arrival_to_date
,       dt.allow_amt
,       dt.amt_in_cost
FROM    (
        SELECT  division_id
        ,       corp_item_cd
        ,       cic_dsc
        ,       group_id
        ,       category_id
        ,       vend_conv_fctr
        ,       pack_retail_qty
        ,       pack_whse_qty
        ,       pack_dsc
        ,       size_dsc
        ,       dst_cntr
        ,       cic_vend_cst
        ,       corporation_id
        ,       whse_cd
        ,       status_dst
        ,       cic_ib_cst
        ,       item_type_cd
        ,       rog_id
        ,       rog_cd
        ,       retail_status_cd
        ,       loc_common_retail_cd
        ,       upc_id
        ,       snstv_tier_cd
        ,       div_promo_grp_cd
        ,       vend_nbr
        ,       vend_sub_acct_nbr
        ,       loc_retail_sect_id
        ,       buyer_nbr
        ,       unit_prc_tbl_nbr
        ,       price_area_id
        ,       cost_area_id
        ,       buyer_nm
        ,       vend_nm
        ,       adplan_flag
        ,       up_mult_factor
        ,       up_label_unit
        ,       pending_cost
        ,       pending_cost_date
        ,       alw_typ
        ,       arrival_from_date
        ,       arrival_to_date
        ,       allow_amt
        ,       amt_in_cost
        FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_4
        GROUP   BY division_id
        ,       corp_item_cd
        ,       cic_dsc
        ,       group_id
        ,       category_id
        ,       vend_conv_fctr
        ,       pack_retail_qty
        ,       pack_whse_qty
        ,       pack_dsc
        ,       size_dsc
        ,       dst_cntr
        ,       cic_vend_cst
        ,       corporation_id
        ,       whse_cd
        ,       status_dst
        ,       cic_ib_cst
        ,       item_type_cd
        ,       rog_id
        ,       rog_cd
        ,       retail_status_cd
        ,       loc_common_retail_cd
        ,       upc_id
        ,       snstv_tier_cd
        ,       div_promo_grp_cd
        ,       vend_nbr
        ,       vend_sub_acct_nbr
        ,       loc_retail_sect_id
        ,       buyer_nbr
        ,       unit_prc_tbl_nbr
        ,       price_area_id
        ,       cost_area_id
        ,       buyer_nm
        ,       vend_nm
        ,       first_eff_dt
        ,       last_eff_dt
        ,       adplan_flag
        ,       up_mult_factor
        ,       up_label_unit
        ,       pending_cost
        ,       pending_cost_date
        ,       alw_typ
        ,       arrival_from_date
        ,       arrival_to_date
        ,       allow_amt
        ,       amt_in_cost
        ) dt
LEFT    OUTER JOIN (
        SELECT  rog_cd
        ,       upc_id
        ,       price_area_id
        ,       reg_rtl_prc
        ,       reg_rtl_prc_fctr
        FROM    (
                SELECT  rog_cd
                ,       upc_id
                ,       price_area_id
                ,       reg_rtl_prc
                ,       reg_rtl_prc_fctr
                ,       COUNT(1) AS cnt
                FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
                WHERE   reg_rtl_prc_fctr > 0
                GROUP   BY rog_cd
                ,       upc_id
                ,       price_area_id
                ,       reg_rtl_prc
                ,       reg_rtl_prc_fctr
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_cd, upc_id, price_area_id ORDER BY cnt DESC) = 1
        ) reg
ON      reg.rog_cd = dt.rog_cd
AND     reg.upc_id = dt.upc_id
AND     reg.price_area_id = dt.price_area_id
LEFT    OUTER JOIN (
        SELECT  rog_cd
        ,       upc_id
        ,       price_area_Id
        ,       offer_id
        ,       COUPON_AMT
        ,       promo_method_cd
        ,       promo_min_purch_qty
        ,       promo_lim_qty
        ,       promo_prc
        ,       promo_prc_fctr
        ,       first_eff_dt
        ,       last_eff_dt
        FROM    (
                SELECT  rog_cd
                ,       upc_id
                ,       price_area_Id
                ,       offer_id
                ,       COUPON_AMT
                ,       promo_method_cd
                ,       promo_min_purch_qty
                ,       promo_lim_qty
                ,       promo_prc
                ,       promo_prc_fctr
                ,       first_eff_dt
                ,       last_eff_dt
                ,       COUNT(1) AS cnt
                FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
                WHERE   offer_id IS NOT NULL
                GROUP   BY rog_cd
                ,       upc_id
                ,       price_area_Id
                ,       offer_id
                ,       COUPON_AMT
                ,       promo_method_cd
                ,       promo_min_purch_qty
                ,       promo_lim_qty
                ,       promo_prc
                ,       promo_prc_fctr
                ,       first_eff_dt
                ,       last_eff_dt
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_cd, upc_id, price_area_id ORDER BY cnt DESC, coupon_amt ASC, promo_prc ASC, offer_id ASC) = 1
        ) promo
ON      promo.rog_cd = dt.rog_cd
AND     promo.upc_id = dt.upc_id
AND     promo.price_area_id = dt.price_area_id
LEFT    OUTER JOIN (
        SELECT  rog_cd
        ,       upc_id
        ,       price_area_id
        ,       prtl_first_eff_dt
        ,       prtl_reg_rtl_prc
        ,       prtl_reg_rtl_prc_fctr
        FROM    (
                SELECT  rog_cd
                ,       upc_id
                ,       price_area_id
                ,       prtl_first_eff_dt
                ,       prtl_reg_rtl_prc
                ,       prtl_reg_rtl_prc_fctr
                ,       COUNT(1) AS cnt
                FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
                WHERE   prtl_reg_rtl_prc_fctr > 0
                GROUP   BY rog_cd
                ,       upc_id
                ,       price_area_id
                ,       prtl_first_eff_dt
                ,       prtl_reg_rtl_prc
                ,       prtl_reg_rtl_prc_fctr
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_cd, upc_id, price_area_id ORDER BY cnt DESC, prtl_reg_rtl_prc ASC, prtl_first_eff_dt ASC) = 1
        ) prtl
ON      prtl.rog_cd = dt.rog_cd
AND     prtl.upc_id = dt.upc_Id
AND     prtl.price_area_id = dt.price_area_id
LEFT    OUTER JOIN (
        SELECT  rog_cd
        ,       upc_id
        ,       price_area_id
        ,       hist_reg_rtl_prc
        ,       hist_reg_rtl_prc_fctr
        ,       hist_last_eff_dt
        FROM    (
                SELECT  rog_cd
                ,       upc_id
                ,       price_area_id
                ,       hist_reg_rtl_prc
                ,       hist_reg_rtl_prc_fctr
                ,       hist_last_eff_dt
                ,       COUNT(1) AS cnt
                FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3
                WHERE   hist_reg_rtl_prc_fctr > 0
                GROUP   BY rog_cd
                ,       upc_id
                ,       price_area_id
                ,       hist_reg_rtl_prc
                ,       hist_reg_rtl_prc_fctr
                ,       hist_last_eff_dt
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_cd, upc_id, price_area_id ORDER BY cnt DESC, hist_last_eff_dt DESC, hist_reg_rtl_prc ASC) = 1
        ) hreg
ON      hreg.rog_cd = dt.rog_cd
AND     hreg.upc_id = dt.upc_Id
AND     hreg.price_area_id = dt.price_area_id;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      promo_no_allowance CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      allow_no_promo CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      missing_allowance CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cost_change CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ad_plan CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      less_10_Promo CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      less_10_allowance CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      greater_100_pass_through CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      t_09_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      lead_item CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Dominant_Price_Area CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OOB CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sskvi CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      group_cd SMALLINT,
      group_nm CHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      SMIC CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      SMIC_name CHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      price_area_id INTEGER,
      PA_name CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pricing_role CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      OOB_gap_id CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      loc_common_retail_cd DECIMAL(5,0),
      vendor_name VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_NBR VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      VEND_SUB_ACCT_NBR VARCHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      cost_area_id SMALLINT,
      Manuf CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      upc_id DECIMAL(13,0),
      corp_item_cd DECIMAL(8,0),
      item_description VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      DST VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      FACILITY VARCHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_stat CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      rtl_stat CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_conv_fctr SMALLINT,
      t_pack_whse_qty DECIMAL(10,0),
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      Row_Offset CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      UPC_13_Wk_Avg_Sales CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      UPC_13_Wk_Avg_Qty CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      UPC_13_Wk_Avg_RTL CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      t_Rank CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pct_ACV_Stores CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CPC_13_Wk_Avg_Sales CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CPC_13_Wk_Avg_Qty CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CPC_13_Wk_Avg_RTL CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PND_Cost_Change_VND DECIMAL(15,4),
      PND_VEN_Date_Effective DATE FORMAT 'YY/MM/DD',
      New_Recc_Reg_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Vendor_Unit_Cost DECIMAL(10,3),
      Unit_Item_Billing_Cost DECIMAL(10,3),
      Prev_Retail_Price_Fctr DECIMAL(2,0),
      Previous_Retail_Price DECIMAL(7,2),
      Prev_Retail_Effective_Date DATE FORMAT 'YY/MM/DD',
      Pending_EDLP_Mult DECIMAL(2,0),
      Pending_EDLP_Retail DECIMAL(7,2),
      Pending_EDLP_Chg_Date DATE FORMAT 'YY/MM/DD',
      Pending_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Reg_Retail_Price_Fctr DECIMAL(2,0),
      Reg_Retail DECIMAL(10,2),
      t_price_Per DECIMAL(10,3),
      t_Unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      Reg_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      case_allow_count INTEGER,
      case_allow_amt DECIMAL(15,4),
      Case_Allow_amt_per_Unit DECIMAL(15,4),
      Case_Start_Date DATE FORMAT 'YY/MM/DD',
      Case_End_Date DATE FORMAT 'YY/MM/DD',
      S2S_Allow_count INTEGER,
      S2S_Allow_amt DECIMAL(15,4),
      S2S_Allow_amt_per_Unit DECIMAL(15,4),
      S2S_Start_Date DATE FORMAT 'YY/MM/DD',
      S2S_End_Date DATE FORMAT 'YY/MM/DD',
      Scan_Allow_count INTEGER,
      Scan_Allow_amt DECIMAL(15,4),
      Scan_Start_Date DATE FORMAT 'YY/MM/DD',
      Scan_End_Date DATE FORMAT 'YY/MM/DD',
      Redem_Scan_Allow_count INTEGER,
      Redem_Allow_amt DECIMAL(15,4),
      Redem_Start_Date DATE FORMAT 'YY/MM/DD',
      Redem_End_Date DATE FORMAT 'YY/MM/DD',
      Total_Allow_Unit CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Allowance_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Net_Cost_with_Allow CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Promo_Multiple DECIMAL(3,0),
      Promo_Price DECIMAL(10,4),
      Coupon_Method CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      Min_Purch DECIMAL(2,0),
      Limit_Per_Txn SMALLINT,
      Promo_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Net_Promo_Price DECIMAL(10,2),
      Price_Per DECIMAL(10,3),
      t2_Unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      Markdown_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Mark_down DECIMAL(10,2),
      Promo_Start DATE FORMAT 'YY/MM/DD',
      Promo_End DATE FORMAT 'YY/MM/DD',
      Pass_Through CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Multiple CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_EDLP_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_Multiple CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_GPpctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Passthrough CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      compet_code VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      price_chk_date DATE FORMAT 'YY/MM/DD',
      comp_reg_mult DECIMAL(2,0),
      com_reg_price DECIMAL(7,2),
      REG_CPI CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      COMP_AD_MULT DECIMAL(2,0),
      COMP_AD_PRICE DECIMAL(7,2),
      Comments CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Modified_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ROG_and_CIG VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Allowance_Counts CHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      Report_Date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( rog_cd ,upc_id );
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6
(       division_id
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog_cd
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       DIV_PROMO_GRP_CD
,       loc_common_retail_cd
,       vendor_name
,       VEND_NBR
,       VEND_SUB_ACCT_NBR
,       cost_area_id
,       Manuf
,       upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       t_pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       t_Rank
,       pct_ACV_Stores
,       CPC_13_Wk_Avg_Sales
,       CPC_13_Wk_Avg_Qty
,       CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
,       Unit_Item_Billing_Cost
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per
,       t_Unit
,       Reg_GP_pctg
,       case_allow_count
,       case_allow_amt
,       Case_Allow_amt_per_Unit
,       Case_Start_Date
,       Case_End_Date
,       S2S_Allow_count
,       S2S_Allow_amt
,       S2S_Allow_amt_per_Unit
,       S2S_Start_Date
,       S2S_End_Date
,       Scan_Allow_count
,       Scan_Allow_amt
,       Scan_Start_Date
,       Scan_End_Date
,       Redem_Scan_Allow_count
,       Redem_Allow_amt
,       Redem_Start_Date
,       Redem_End_Date
,       Total_Allow_Unit
,       Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       Markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       Pass_Through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code
,       price_chk_date
,       comp_reg_mult
,       com_reg_price
,       REG_CPI
,       COMP_AD_MULT
,       COMP_AD_PRICE
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Allowance_Counts
,       Report_Date
)
SELECT  t.division_id
,       TRIM(' ') (CHAR(1)) AS promo_no_allowance
,       TRIM(' ') (CHAR(1)) AS allow_no_promo
,       TRIM(' ') (CHAR(1)) AS missing_allowance
,       TRIM(' ') (CHAR(1)) AS cost_change
,       adplan_flag (CHAR(1)) AS ad_plan
,       TRIM(' ') (CHAR(1)) AS less_10_Promo
,       TRIM(' ') (CHAR(1)) AS less_10_allowance
,       TRIM(' ') (CHAR(1)) AS greater_100_pass_through
,       TRIM(' ') (CHAR(1)) AS t_09_Retail
,       TRIM(' ') (CHAR(1)) AS lead_item
,       CASE
            WHEN rog_cd = 'SEAS' AND price_area_id = 36 THEN 'Y'     -- SEAS
            WHEN rog_cd = 'SEAG' AND price_area_id = 2  THEN 'Y'     -- SEAG
            WHEN rog_cd = 'ACME' AND  price_area_id = 87    THEN 'Y' -- ACME
            WHEN rog_cd = 'SDEN' AND  price_area_id = 71    THEN 'Y' -- SDEN
            WHEN rog_cd = 'ADEN' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'AIMT' AND  price_area_id = 6 THEN 'Y'
            WHEN rog_cd = 'AJWL' AND  price_area_id = 7 THEN 'Y'
            WHEN rog_cd = 'SHAW' AND  price_area_id = 47 THEN 'Y'
            WHEN rog_cd = 'SNCA' AND  price_area_id = 11 THEN 'Y'
            WHEN rog_cd = 'SPRT' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'APOR' AND  price_area_id = 4 THEN 'Y'
            WHEN rog_cd = 'SSEA' AND  price_area_id = 47 THEN 'Y'
            WHEN rog_cd = 'SSPK' AND  price_area_id = 60 THEN 'Y'
            WHEN rog_cd = 'SACG' AND  price_area_id = 8 THEN 'Y'
            WHEN rog_cd = 'ASHA' AND  price_area_id = 23 THEN 'Y'
            WHEN rog_cd = 'AVMT' AND  price_area_id = 32 THEN 'Y'
            WHEN rog_cd = 'VSOC' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'ASOC' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'PSOC' AND  price_area_id = 89 THEN 'Y'
            WHEN rog_cd = 'RHOU' AND  price_area_id = 33 THEN 'Y'
            WHEN rog_cd = 'RDAL' AND  price_area_id = 83 THEN 'Y'
            WHEN rog_cd = 'ADAL' AND  price_area_id = 4 THEN 'Y'
            WHEN rog_cd = 'AHOU' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'ALAS' AND  price_area_id = 71 THEN 'Y'
            WHEN rog_cd = 'APHO' AND  price_area_id = 41 THEN 'Y'
            WHEN rog_cd = 'SPHO' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'SPHX' AND  price_area_id = 1 THEN 'Y'
            WHEN rog_cd = 'VLAS' AND  price_area_id = 62 THEN 'Y'
            ELSE ' '
        END (CHAR(1)) AS Dominant_Price_Area
,       TRIM(' ') (CHAR(1)) AS OOB
,       TRIM(' ') (CHAR(1)) AS sskvi
,       t.group_id (FORMAT '99') group_cd
,       grp.group_nm
,       t.category_id (FORMAT '9999') (CHAR(4)) AS SMIC
,       cat.category_nm AS SMIC_name
,       rog_cd
,       price_area_id
,       TRIM(' ') (CHAR(1)) AS PA_name
,       snstv_tier_cd AS pricing_role
,       ((t.group_id (FORMAT '99') (CHAR(2))) || '-' || TRIM(item_type_cd (FORMAT '999') (CHAR(3)))) (CHAR(20)) AS OOB_gap_id  -- change "|" to "-"
,       div_promo_grp_cd  -- CIG
,       loc_common_retail_cd  -- COMMON_CD
,       COALESCE(vend_nm, '') AS vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       SUBSTR(t.upc_id (FORMAT '999999999999') (CHAR(12)), 3, 5) (CHAR(5)) AS Manuf
,       t.upc_id
,       corp_item_cd
,       cic_dsc AS item_description
,       dst_cntr AS DST
,       whse_cd AS FACILITY
,       status_dst AS dst_stat
,       retail_status_cd AS rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       pack_whse_qty (DECIMAL(10,0)) AS t_pack_whse_qty
,       size_dsc
,       TRIM(' ') (CHAR(1)) AS Row_Offset
,       TRIM(' ') (CHAR(1)) AS UPC_13_Wk_Avg_Sales
,       TRIM(' ') (CHAR(1)) AS UPC_13_Wk_Avg_Qty
,       TRIM(' ') (CHAR(1)) AS UPC_13_Wk_Avg_RTL
,       TRIM(' ') (CHAR(1)) AS t_Rank
,       TRIM(' ') (CHAR(1)) AS pct_ACV_Stores
,       TRIM(' ') (CHAR(1)) AS CPC_13_Wk_Avg_Sales
,       TRIM(' ') (CHAR(1)) AS CPC_13_Wk_Avg_Qty
,       TRIM(' ') (CHAR(1)) AS CPC_13_Wk_Avg_RTL
,       pending_cost / vend_conv_fctr / pack_whse_qty AS PND_Cost_Change_VND
,       pending_cost_date AS PND_VEN_Date_Effective
,       TRIM(' ') (CHAR(1)) AS New_Recc_Reg_Retail
,       (cic_vend_cst / vend_conv_fctr / pack_whse_qty) (DECIMAL(10,3)) AS Vendor_Unit_Cost
,       (cic_ib_cst / pack_whse_qty) (DECIMAL(10,3)) AS Unit_Item_Billing_Cost
,       hist_reg_rtl_prc_fctr AS Prev_Retail_Price_Fctr
,       hist_reg_rtl_prc AS Previous_Retail_Price
,       hist_last_eff_dt AS Prev_Retail_Effective_Date
,       prtl_reg_rtl_prc_fctr AS Pending_EDLP_Mult
,       prtl_reg_rtl_prc AS Pending_EDLP_Retail
,       prtl_first_eff_dt AS Pending_EDLP_Chg_Date
,       TRIM(' ') (CHAR(1)) AS Pending_GP_pctg
,       reg_rtl_prc_fctr AS Reg_Retail_Price_Fctr
,       reg_rtl_prc (DECIMAL(10,2)) AS Reg_Retail
,       (reg_rtl_prc / reg_rtl_prc_fctr / 1 / up_mult_factor) (DECIMAL(10,3)) AS t_price_Per -- missing SSITMPOS.up_measure
--, Decimal(Round(promo_prc / promo_prc_fctr /
--  ITM.UP_MEASURE * up_mult_factor, 2), 10, 3) AS "Price_Per..."
,       CASE
            WHEN reg_rtl_prc IS NOT NULL THEN up_label_unit
            ELSE ''
        END AS t_Unit
,       TRIM(' ') (CHAR(1)) AS Reg_GP_pctg
,       COUNT(CASE WHEN ALW_TYP = 'C' THEN ALLOW_AMT END) AS case_allow_count
,       Sum (CASE  WHEN ALW_TYP = 'C' THEN ALLOW_AMT ELSE 0 END) AS case_allow_amt
,       Sum (CASE  WHEN ALW_TYP = 'C' THEN ALLOW_AMT / pack_whse_qty /
            VEND_CONV_FCTR ELSE 0 END) AS Case_Allow_amt_per_Unit
,       Max (CASE  WHEN ALW_TYP = 'C' THEN ARRIVAL_FROM_DATE
            ELSE NULL END) AS Case_Start_Date
,       Min (CASE  WHEN ALW_TYP = 'C' THEN ARRIVAL_TO_DATE
            ELSE NULL END) AS Case_End_Date
,       COUNT(CASE WHEN ALW_TYP = 'S' THEN ALLOW_AMT END) AS S2S_Allow_count
,       Sum (CASE  WHEN ALW_TYP = 'S' THEN ALLOW_AMT ELSE 0 END) AS S2S_Allow_amt
,       Sum (CASE  WHEN ALW_TYP = 'S' THEN ALLOW_AMT / pack_whse_qty
            ELSE 0 END) AS S2S_Allow_amt_per_Unit
,       Max (CASE  WHEN ALW_TYP = 'S' THEN ARRIVAL_FROM_DATE
            ELSE NULL END) AS S2S_Start_Date
,       Min (CASE  WHEN ALW_TYP = 'S' THEN ARRIVAL_TO_DATE
            ELSE NULL END) AS S2S_End_Date
,       COUNT(CASE WHEN ALW_TYP = 'T' THEN ALLOW_AMT END) AS Scan_Allow_count
,       Sum (CASE  WHEN ALW_TYP = 'T' THEN ALLOW_AMT ELSE 0 END) AS Scan_Allow_amt
,       Max (CASE  WHEN ALW_TYP = 'T' THEN ARRIVAL_FROM_DATE
            ELSE NULL END) AS Scan_Start_Date
,       Min (CASE  WHEN ALW_TYP = 'T' THEN ARRIVAL_TO_DATE
            ELSE NULL END) AS Scan_End_Date
,       COUNT(CASE WHEN ALW_TYP = 'R' THEN ALLOW_AMT END) AS Redem_Scan_Allow_count
,       Sum (CASE  WHEN ALW_TYP = 'R' THEN ALLOW_AMT ELSE 0 END) AS Redem_Allow_amt
,       Max (CASE  WHEN ALW_TYP = 'R' THEN ARRIVAL_FROM_DATE
            ELSE NULL END) AS Redem_Start_Date
,       Min (CASE  WHEN ALW_TYP = 'R' THEN ARRIVAL_TO_DATE
            ELSE NULL END) AS Redem_End_Date
,       TRIM(' ') (CHAR(1)) AS Total_Allow_Unit
,       TRIM(' ') (CHAR(1)) AS Allowance_pctg
,       TRIM(' ') (CHAR(1)) AS Net_Cost_with_Allow
,       CASE WHEN promo_prc_fctr = 0 THEN 1 ELSE promo_prc_fctr END AS Promo_Multiple
,       CASE WHEN promo_prc_fctr = 0 AND coupon_amt > 0 THEN coupon_amt
            ELSE promo_prc + 0.0001 END AS Promo_Price
,       promo_method_cd AS Coupon_Method
,       promo_min_purch_qty AS Min_Purch
,       promo_lim_qty AS Limit_Per_Txn
,       TRIM(' ') (CHAR(1)) AS Promo_GP_pctg
,       CASE
            WHEN promo_method_cd = 'NE' AND coupon_amt > 0 THEN coupon_amt
            WHEN promo_method_cd = 'NW' AND coupon_amt > 0 THEN coupon_amt
            WHEN promo_method_cd = 'NE' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) -
                 ( (reg_rtl_prc / reg_rtl_prc_fctr) / promo_min_purch_qty )
            WHEN promo_method_cd = 'NW' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) -
                 ( (reg_rtl_prc / reg_rtl_prc_fctr) / promo_min_purch_qty )
            WHEN promo_method_cd = 'CE' AND promo_min_purch_qty = 1 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
            WHEN promo_method_cd = 'CW' AND promo_min_purch_qty = 1 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
            WHEN promo_method_cd = 'CE' AND promo_min_purch_qty > 1 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) -
                 ( coupon_amt / promo_min_purch_qty )
            WHEN promo_method_cd = 'CW' AND promo_min_purch_qty > 1 THEN
                 (reg_rtl_prc / reg_rtl_prc_fctr) -
                 ( coupon_amt / promo_min_purch_qty )
            WHEN promo_method_cd = 'PE' THEN (reg_rtl_prc/reg_rtl_prc_fctr)-
                 ( ( (reg_rtl_prc / reg_rtl_prc_fctr)*coupon_amt)/100)
            WHEN promo_method_cd = 'PW' THEN (reg_rtl_prc/reg_rtl_prc_fctr)-
                 ( ( (reg_rtl_prc / reg_rtl_prc_fctr)*coupon_amt )/100)
            ELSE 0
        END (DECIMAL(10,2)) AS Net_Promo_Price -- (DECIMAL(10,2)) AS Net_Promo_Price
,       (CASE
                WHEN promo_method_cd ='NE' AND coupon_amt > 0 THEN coupon_amt
                WHEN promo_method_cd ='NW' AND coupon_amt > 0 THEN coupon_amt
                WHEN promo_method_cd ='NE' AND promo_min_purch_qty>1 AND coupon_amt=0 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                   ( (reg_rtl_prc / reg_rtl_prc_fctr) /
                   promo_min_purch_qty )
                WHEN promo_method_cd ='NW' AND promo_min_purch_qty>1 AND coupon_amt=0 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                 ((reg_rtl_prc/reg_rtl_prc_fctr)/promo_min_purch_qty)
                WHEN promo_method_cd ='CE' AND promo_min_purch_qty = 1 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr)-coupon_amt
                WHEN promo_method_cd ='CW' AND promo_min_purch_qty = 1 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr)-coupon_amt
                WHEN promo_method_cd ='CE' AND promo_min_purch_qty > 1 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                   ( coupon_amt / promo_min_purch_qty )
                WHEN promo_method_cd ='CW' AND promo_min_purch_qty > 1 THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                   ( coupon_amt / promo_min_purch_qty )
                WHEN promo_method_cd ='PE' THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                   ( ( (reg_rtl_prc / reg_rtl_prc_fctr) *
                   coupon_amt ) / 100 )
                WHEN promo_method_cd ='PW' THEN
                   (reg_rtl_prc / reg_rtl_prc_fctr) -
                   ( ( (reg_rtl_prc / reg_rtl_prc_fctr) *
                   coupon_amt ) / 100 )
                ELSE 0
        END / (CASE WHEN promo_prc_fctr = 0 THEN 1 ELSE promo_prc_fctr END)
        / 1 * up_mult_factor) (DECIMAL(10,3)) AS Price_Per -- (DECIMAL(10,3)) AS Price_Per
--                        / ITM.UP_MEASURE * up_mult_factor AS "Price_Per...."
,       CASE
            WHEN (
                CASE
                    WHEN promo_method_cd = 'NE' AND coupon_amt > 0 THEN coupon_amt
                    WHEN promo_method_cd = 'NW' AND coupon_amt > 0 THEN coupon_amt
                    WHEN promo_method_cd = 'NE' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( (reg_rtl_prc / reg_rtl_prc_fctr) / promo_min_purch_qty )
                    WHEN promo_method_cd = 'NW' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( (reg_rtl_prc / reg_rtl_prc_fctr) / promo_min_purch_qty )
                    WHEN promo_method_cd = 'CE' AND promo_min_purch_qty = 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
                    WHEN promo_method_cd = 'CW' AND promo_min_purch_qty = 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
                    WHEN promo_method_cd = 'CE' AND promo_min_purch_qty > 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( coupon_amt / promo_min_purch_qty )
                    WHEN promo_method_cd = 'CW' AND promo_min_purch_qty > 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( coupon_amt / promo_min_purch_qty )
                    WHEN promo_method_cd = 'PE' THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        (((reg_rtl_prc/reg_rtl_prc_fctr)*coupon_amt )/ 100)
                    WHEN promo_method_cd = 'PW' THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        (((reg_rtl_prc/reg_rtl_prc_fctr)*coupon_amt)/100)
                    ELSE 0
                END) <> 0 THEN up_label_unit
            ELSE NULL --TRIM(' ')
        END AS t2_Unit
,       TRIM(' ') (CHAR(1)) AS Markdown_pctg
,       CASE
            WHEN NOT promo_method_cd IS NULL THEN (reg_rtl_prc / reg_rtl_prc_fctr) -
                (
                CASE
                    WHEN promo_method_cd = 'NE' AND coupon_amt > 0 THEN coupon_amt
                    WHEN promo_method_cd = 'NW' AND coupon_amt > 0 THEN coupon_amt
                    WHEN promo_method_cd = 'NE' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                        ( reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( (reg_rtl_prc / reg_rtl_prc_fctr) / promo_min_purch_qty )
                    WHEN promo_method_cd = 'NW' AND promo_min_purch_qty > 1 AND coupon_amt = 0 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) - ( (reg_rtl_prc /
                        reg_rtl_prc_fctr) / promo_min_purch_qty )
                    WHEN promo_method_cd = 'CE' AND promo_min_purch_qty = 1 THEN
                        ( reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
                    WHEN promo_method_cd = 'CW' AND promo_min_purch_qty = 1 THEN
                        ( reg_rtl_prc / reg_rtl_prc_fctr) - coupon_amt
                    WHEN promo_method_cd = 'CE' AND promo_min_purch_qty > 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) - ( coupon_amt / promo_min_purch_qty )
                    WHEN promo_method_cd = 'CW' AND promo_min_purch_qty > 1 THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) - ( coupon_amt / promo_min_purch_qty )
                    WHEN promo_method_cd = 'PE' THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( ( (reg_rtl_prc / reg_rtl_prc_fctr) * coupon_amt ) / 100 )
                    WHEN promo_method_cd = 'PW' THEN
                        (reg_rtl_prc / reg_rtl_prc_fctr) -
                        ( ( (reg_rtl_prc / reg_rtl_prc_fctr) * coupon_amt ) / 100 )
                    ELSE 0
                END)
            ELSE NULL
        END (DECIMAL(10,2)) AS Mark_down
,       first_eff_dt AS Promo_Start
,       last_eff_dt AS Promo_End
,       TRIM(' ') (CHAR(1)) AS Pass_Through
,       TRIM(' ') (CHAR(1)) AS NEW_Multiple
,       TRIM(' ') (CHAR(1)) AS NEW_Retail
,       TRIM(' ') (CHAR(1)) AS NEW_EDLP_GP_pctg
,       TRIM(' ') (CHAR(1)) AS NEW_Promo_Multiple
,       TRIM(' ') (CHAR(1)) AS NEW_Promo_Retail
,       TRIM(' ') (CHAR(1)) AS NEW_Promo_GPpctg
,       TRIM(' ') (CHAR(1)) AS NEW_Passthrough
,       cmp.COMPET_CODE AS compet_code  --TRIM(TB2.COMPET_CODE) AS "COMPET_CODE"
,       cmp.PRICE_CHK_DATE AS price_chk_date --TB2.PRICE_CHK_DATE AS "DATE"
,       cmp.CMP_PRICE_FCTR AS comp_reg_mult -- TB2.CMP_PRICE_FCTR AS "COMP_REG_MULT"
,       cmp.CMP_PRICE AS com_reg_price -- (TB2.CMP_PRICE) AS "COMP_REG_PRICE"
,       TRIM(' ') (CHAR(1)) AS REG_CPI
,       cmp.CMP_SHELF_PRC_FCTR AS COMP_AD_MULT --TB2.CMP_SHELF_PRC_FCTR AS "COMP_AD_MULT"
,       cmp.CMP_SHELF_PRICE AS COMP_AD_PRICE --(TB2.CMP_SHELF_PRICE) AS "COMP_AD_PRICE"
,       TRIM(' ') (CHAR(1)) AS Comments
,       TRIM(' ') (CHAR(1)) AS Modified_flag
,       CASE
            WHEN rog_cd = 'SEAS' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SEAS' || '-'
            WHEN rog_cd = 'SEAG' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-SEAG' || '-'
            WHEN rog_cd = 'ACME' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-ACME' || '-'
            WHEN rog_cd = 'SDEN' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SDEN' || '-'
            WHEN rog_cd = 'ADEN' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-ADEN' || '-'
            WHEN rog_cd = 'AIMT' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-AIMT' || '-'
            WHEN rog_cd = 'AJWL' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-AJWL' || '-'
            WHEN rog_cd = 'SNCA' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SNCA' || '-'
            WHEN rog_cd = 'SHAW' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-SHAW' || '-'
            WHEN rog_cd = 'SPRT' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SPRT' || '-'
            WHEN rog_cd = 'APOR' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-APOR' || '-'
            WHEN rog_cd = 'SSEA' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SSEA' || '-'
            WHEN rog_cd = 'SSPK' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-SSPK' || '-'
            WHEN rog_cd = 'SACG' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '3-SACG' || '-'
            WHEN rog_cd = 'ASHA' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-ASHA' || '-'
            WHEN rog_cd = 'AVMT' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-AVMT' || '-'
            WHEN rog_cd = 'VSOC' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-VSOC' || '-'
            WHEN rog_cd = 'ASOC' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-ASOC' || '-'
            WHEN rog_cd = 'PSOC' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '3-PSOC' || '-'
            WHEN rog_cd = 'RDAL' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-RDAL' || '-'
            WHEN rog_cd = 'ADAL' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-ADAL' || '-'
            WHEN rog_cd = 'RHOU' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '3-RHOU' || '-'
            WHEN rog_cd = 'AHOU' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '4-AHOU' || '-'
            WHEN rog_cd = 'SPHO' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '1-SPHO' || '-'
            WHEN rog_cd = 'APHO' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '2-APHO' || '-'
            WHEN rog_cd = 'ALAS' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '3-ALAS' || '-'
            WHEN rog_cd = 'VLAS' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '4-VLAS' || '-'
            WHEN rog_cd = 'SPHX' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '5-SPHX' || '-'
            WHEN rog_cd = 'TEST' AND DIV_PROMO_GRP_CD IS NOT NULL THEN '6-TEST' || '-'
            ELSE TRIM(' ')
        END || COALESCE(TRIM(CAST(DIV_PROMO_GRP_CD AS CHAR(11))), TRIM(' ')) (VARCHAR(15)) AS ROG_and_CIG
,       TRIM(COUNT(CASE WHEN ALW_TYP = 'C' THEN ALLOW_AMT END)) || '-' ||
        TRIM(COUNT(CASE WHEN ALW_TYP = 'S' THEN ALLOW_AMT END)) || '-' ||
        TRIM(COUNT(CASE WHEN ALW_TYP = 'T' THEN ALLOW_AMT END)) || '-' ||
        TRIM(COUNT(CASE WHEN ALW_TYP = 'R' THEN ALLOW_AMT END)) (CHAR(50)) AS Allowance_Counts
,       DATE '${rptDate}' AS Report_Date  --Max(CURRENT DATE+(13-Dayofweek(CURRENT DATE+2 DAY))DAY) AS "Report Date"
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_5 t
INNER   JOIN ${DWH_DSS_DB}.lu_group grp
ON      grp.group_id = t.group_id
INNER   JOIN ${DWH_DSS_DB}.lu_category cat
ON      cat.category_id = t.category_id
LEFT    OUTER JOIN (
        SELECT  ((UPC_COUNTRY_CD (FORMAT '9') (CHAR(1))) || (UPC_SYSTEM_ID (FORMAT '9') (CHAR(1))) || (UPC_MANUF_CD (FORMAT '99999') (CHAR(5))) || (UPC_SALES_CD (FORMAT '99999') (CHAR(5)))) (DECIMAL(14,0)) AS upc_id
        ,       cmp_store_id    AS COMPET_FACILITY
        ,       cmp_prc_fctr    AS CMP_PRICE_FCTR
        ,       cmp_prc_amt     AS CMP_PRICE
        ,       cmp_shelf_prc_fctr  AS CMP_SHELF_PRC_FCTR
        ,       cmp_shelf_prc_amt   AS CMP_SHELF_PRICE
        ,       price_chk_dt        AS PRICE_CHK_DATE
        ,       cmp_cd              AS COMPET_CODE
        -- FROM temp_sfwy3.SMCMPSUM_20181121    -- CHANGE ME:  ${DWH_DSS_DB}.competitor_swy_item_prc
        FROM    ${DWH_DSS_DB}.competitor_swy_item_prc
        GROUP   BY 1,2,3,4,5,6,7,8
        ) cmp
ON      cmp.upc_id = t.upc_id
AND     cmp.compet_facility = (
        CASE WHEN t.ROG_CD = 'SEAG' THEN '7319'
            WHEN t.ROG_CD = 'SEAS' AND PRICE_AREA_ID = 4 THEN '1806'
            WHEN t.ROG_CD = 'SEAS' AND PRICE_AREA_ID <> 4 THEN '7319'

            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (2,3,5) THEN '9503'
            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (4) THEN '9504'
            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (6, 87) THEN '9587'
            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (7, 9) THEN '9509'
            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (8) THEN '9508'
            WHEN t.ROG_CD = 'ACME' AND PRICE_AREA_ID IN (88, 89) THEN '9589'

            WHEN t.ROG_CD = 'ADEN' AND PRICE_AREA_ID IN (1) THEN '2039'
            WHEN t.ROG_CD = 'ADEN' AND PRICE_AREA_ID IN (2, 3) THEN '2000'
            WHEN t.ROG_CD = 'SDEN' AND PRICE_AREA_ID IN (12) THEN '2650'
            WHEN t.ROG_CD = 'SDEN' AND PRICE_AREA_ID IN (13, 41, 51, 55, 61, 70, 71, 95) THEN '2000'

            WHEN t.ROG_CD = 'AIMT' AND PRICE_AREA_ID IN (1, 2, 5) THEN '9119'
            WHEN t.ROG_CD = 'AIMT' AND PRICE_AREA_ID IN (4) THEN '9112'
            WHEN t.ROG_CD = 'AIMT' AND PRICE_AREA_ID IN (6) THEN '9111'
            WHEN t.ROG_CD = 'AIMT' AND PRICE_AREA_ID IN (8) THEN '9124'
            WHEN t.ROG_CD = 'AIMT' AND PRICE_AREA_ID IN (14) THEN '9128'

            WHEN t.ROG_CD = 'AJWL' THEN '9999'

            WHEN t.ROG_CD = 'SHAW' AND PRICE_AREA_ID IN (47) THEN '5601'
            WHEN t.ROG_CD = 'SNCA' AND PRICE_AREA_ID IN (1) THEN '4194'
            WHEN t.ROG_CD = 'SNCA' AND PRICE_AREA_ID IN (11) THEN '4115'
            WHEN t.ROG_CD = 'SNCA' AND PRICE_AREA_ID IN (17) THEN '4128'
            WHEN t.ROG_CD = 'SNCA' AND PRICE_AREA_ID IN (18) THEN '4295'

            WHEN t.ROG_CD = 'SPRT' AND PRICE_AREA_ID IN (1, 61) THEN '9502'
            WHEN t.ROG_CD = 'SPRT' AND PRICE_AREA_ID NOT IN (1, 61) THEN '3500'
            WHEN t.ROG_CD = 'APOR' THEN '3500'

            WHEN t.ROG_CD = 'SSEA' AND PRICE_AREA_ID IN (47,73, 42,53) THEN '6185'
            WHEN t.ROG_CD = 'SSEA' AND PRICE_AREA_ID IN (33,43) THEN '6120'
            WHEN t.ROG_CD = 'SSEA' AND PRICE_AREA_ID IN (34) THEN '6134'
            WHEN t.ROG_CD = 'SSPK' AND PRICE_AREA_ID IN (60,62,63,65) THEN '6160'
            WHEN t.ROG_CD = 'SSPK' AND PRICE_AREA_ID IN (71) THEN '6161'
            WHEN t.ROG_CD = 'SSPK' AND PRICE_AREA_ID IN (72) THEN '6673'
            WHEN t.ROG_CD = 'SACG' AND PRICE_AREA_ID IN (8,10,16) THEN '6108'

            WHEN t.ROG_CD = 'ASHA' AND PRICE_AREA_ID IN (3,4,8,10) THEN '3304'
            WHEN t.ROG_CD = 'ASHA' AND PRICE_AREA_ID IN (5) THEN '3305'
            WHEN t.ROG_CD = 'ASHA' AND PRICE_AREA_ID IN (6,16,22,23) THEN '3301'
            WHEN t.ROG_CD = 'ASHA' AND PRICE_AREA_ID IN (54,61) THEN '3302'
            WHEN t.ROG_CD = 'AVMT' AND PRICE_AREA_ID IN (32,35) THEN '3303'
            WHEN t.ROG_CD = 'AVMT' AND PRICE_AREA_ID IN (58) THEN '3302'

            WHEN t.ROG_CD = 'ASOC' AND PRICE_AREA_ID IN (1,2,4) THEN '9149'
            WHEN t.ROG_CD = 'ASOC' AND PRICE_AREA_ID IN (3,10) THEN '9106'
            WHEN t.ROG_CD = 'VSOC' AND PRICE_AREA_ID IN (1,2,4) THEN '9149'
            WHEN t.ROG_CD = 'VSOC' AND PRICE_AREA_ID IN (3) THEN '9106'
            WHEN t.ROG_CD = 'PSOC' AND PRICE_AREA_ID IN (88,89) THEN '9149'

            WHEN t.ROG_CD = 'RHOU' THEN '1720'
            WHEN t.ROG_CD = 'RDAL' THEN '1724'
            WHEN t.ROG_CD = 'ADAL' AND PRICE_AREA_ID NOT IN (6) THEN '1789'
            WHEN t.ROG_CD = 'ADAL' AND PRICE_AREA_ID IN (6) THEN '1791'
            WHEN t.ROG_CD = 'AHOU' THEN '1804'

            WHEN t.ROG_CD = 'ALAS' AND PRICE_AREA_ID IN (71,72) THEN '9401'
            WHEN t.ROG_CD = 'APHO' AND PRICE_AREA_ID IN (31) THEN '9931'
            WHEN t.ROG_CD = 'APHO' AND PRICE_AREA_ID IN (41) THEN '2749'
            WHEN t.ROG_CD = 'SPHO' AND PRICE_AREA_ID IN (1) THEN '2749'
        END
        )
GROUP   BY division_id
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog_cd
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       div_promo_grp_cd  -- CIG
,       loc_common_retail_cd
,       vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       Manuf
,       t.upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       t_Rank
,       pct_ACV_Stores
,       CPC_13_Wk_Avg_Sales
,       CPC_13_Wk_Avg_Qty
,       CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
--COALESCE (PENDING_VENDOR_COST.COST_VEND, PENDING_WSD_COST.COST_VEND) /
--  VEND_CONV_FCTR / PACK_WHSE AS "PND Cost Change VND"
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
--, Decimal(TB2.COST_VEND/PACK_WHSE/VEND_CONV_FCTR,10,3) AS "Vendor Unit Cost"
,       Unit_Item_Billing_Cost
--, Decimal(TB2.COST_IB / PACK_WHSE, 10, 3) AS "Unit Item Billing Cost"
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per -- missing SSITMPOS.up_measure
--, Decimal(Round(promo_prc / promo_prc_fctr /
--  ITM.UP_MEASURE * up_mult_factor, 2), 10, 3) AS "Price_Per..."
,       t_Unit
--, CASE
--    WHEN TB2.PRICE IS NOT NULL THEN up_label_unit
--    ELSE TRIM(' ')
--  END AS "...Unit"
,       Reg_GP_pctg
,       Total_Allow_Unit
,       Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       Markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       Pass_Through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code  --TRIM(TB2.COMPET_CODE) AS "COMPET_CODE"
,       price_chk_date --TB2.PRICE_CHK_DATE AS "DATE"
,       comp_reg_mult -- TB2.CMP_PRICE_FCTR AS "COMP_REG_MULT"
,       com_reg_price -- (TB2.CMP_PRICE) AS "COMP_REG_PRICE"
,       REG_CPI
,       COMP_AD_MULT --TB2.CMP_SHELF_PRC_FCTR AS "COMP_AD_MULT"
,       COMP_AD_PRICE --(TB2.CMP_SHELF_PRICE) AS "COMP_AD_PRICE"
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Report_Date;
.IF ERRORLEVEL <> 0 THEN .QUIT;

DROP TABLE ${DWH_STAGE_DB}.t_pe_sua_sales;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_sua_sales ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      upc_id DECIMAL(14,0),
      rog_upc VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      upc_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      department_id VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      department_name VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_nm VARCHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      category_id VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      category_nm VARCHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      cpc VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_promo_grp_cd INTEGER,
      rank_by_rog_and_cpc VARCHAR(18) CHARACTER SET LATIN NOT CASESPECIFIC,
      avg_net_sales_13_wk DECIMAL(18,2),
      avg_item_qty_13_wk DECIMAL(18,3),
      num_stores_selling INTEGER,
      num_stores_in_rog INTEGER)
PRIMARY INDEX ( rog_cd ,upc_id );
.IF ERRORLEVEL <> 0 THEN .QUIT;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_sua_sales
(       division_id
,       rog_cd
,       upc_id
,       ROG_UPC
,       upc_dsc
,       department_id
,       department_name
,       group_id
,       group_nm
,       category_id
,       category_nm
,       cpc
,       div_promo_grp_cd
,       rank_by_rog_and_cpc
,       avg_net_sales_13_wk
,       avg_item_qty_13_wk
,       num_stores_selling
,       num_stores_in_rog
)
SELECT  str.division_id
,       str.rog_cd
,       upc.upc_id
,       str.rog_cd || '-' || TRIM(cast(upc.upc_id AS BIGINT)) AS rog_upc
,       upc.upc_dsc
,       TRIM(upc.department_id) AS department_id
,       TRIM(upc.department_nm) AS department_name
,       TRIM(upc.group_id)      AS group_id
,       TRIM(upc.group_nm)      AS group_nm
,       TRIM(upc.category_id)   AS category_id
,       TRIM(upc.category_nm)   AS category_nm
,       CASE WHEN upc.common_promo_cd <> 0
            THEN upc.common_promo_cd ELSE ''
        END AS cpc
,       urx.div_promo_grp_cd
,       CASE
            WHEN upc.common_promo_cd <> 0
                THEN str.rog_cd || '_' || RIGHT('00' || TRIM(ROW_NUMBER() OVER(
                    PARTITION BY str.rog_cd,upc.common_promo_cd
                    ORDER BY avg_net_sales_13_wk DESC)),3)
            ELSE ''
        END AS rank_by_rog_and_cpc
,       SUM(agp.sum_net_amt) /13    AS avg_net_sales_13_wk
,       SUM(agp.sum_item_qty)/13    AS avg_item_qty_13_wk
,       COUNT (UNIQUE str.store_id) AS num_stores_selling
,       tb1.total_stores            AS num_stores_in_rog
FROM    ${DWH_DSS_DB}.store_upc_agp agp
INNER   JOIN ${DWH_DSS_DB}.lu_store str
ON      str.store_id = agp.store_id
AND     str.corporation_id = 1
LEFT    OUTER JOIN (
        SELECT  rog_id
        ,       upc_Id
        ,       div_promo_grp_cd
        FROM    (
                SELECT  rog_id
                ,       upc_id
                ,       div_promo_grp_cd
                ,       COUNT(1) AS cnt
                FROM    ${DWH_DSS_DB}.cic_upc_rog
                WHERE   last_eff_dt = DATE '9999-12-31'
                AND     div_promo_grp_cd > 0
                GROUP   BY 1,2,3
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_id, upc_id ORDER BY cnt DESC, div_promo_grp_cd DESC) = 1
        ) urx
ON      urx.rog_id = str.rog_id
AND     urx.upc_id = agp.upc_id
INNER   JOIN    (
        SELECT  str.rog_cd
        ,       COUNT(UNIQUE str.store_id) AS total_stores
        FROM    ${DWH_DSS_DB}.lu_store str
        WHERE   str.corporation_id = 1
        AND     str.closed_dt > CURRENT_DATE
        AND     str.store_id NOT IN (38,47,339,630,1227,1509,1708,4025,4041,4602  -- div 30 exclusions
                , 1132,1615,2911,2919 -- div 05 exclusions
                , 9835 -- div 35 exclusions
                )
        AND     str.store_id NOT IN (SELECT store_id FROM ${DWH_DSS_DB}.lu_store WHERE division_id = 27 AND district_id = 39)  -- div 27 exclusions
        GROUP   BY str.rog_cd
        ) AS tb1
ON      str.rog_cd = tb1.rog_cd
INNER   JOIN    ${DWH_DSS_DB}.lu_upc upc
ON      upc.upc_id = agp.upc_id
AND     upc.corporation_id = 1
WHERE   upc.category_id NOT IN (1350, 1390,1995,3290,7301,7305,7306,7310,7315,
        7320,7325,7330,7335,7340,7345,7401,7402,7405,
        7410,7415,7416,7420,7425,7430,7445,7450,7470,
        7480,7485,7490)
AND     agp.day_dt BETWEEN CURRENT_DATE - 93 AND CURRENT_DATE - 3
AND     NOT (agp.sum_net_amt <= 0 and agp.sum_item_qty <= 0)
AND     str.closed_dt > CURRENT_DATE
AND     upc.group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37,38,39,40,42,43,44,45,46,47,48,73,74,96,97)
AND     str.store_id NOT IN (38,47,339,630,1227,1509,1708,4025,4041,4602
        , 1132,1615,2911,2919
        , 9835
        )
AND     str.store_id NOT IN (SELECT store_id FROM ${DWH_DSS_DB}.lu_store WHERE division_id = 27 AND district_id = 39)
GROUP   BY str.division_id
,       str.rog_cd
,       upc.upc_id
,       upc.upc_dsc
,       upc.department_id
,       upc.department_nm
,       upc.group_id
,       upc.group_nm
,       upc.category_id
,       upc.category_nm
,       upc.common_promo_cd
,       urx.div_promo_grp_cd
,       tb1.total_stores;
.IF ERRORLEVEL <> 0 THEN .QUIT;

DROP TABLE ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rpt_group VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      rog_cd CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      upc_id DECIMAL(14,0),
      rog_upc VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      upc_dsc VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      department_id VARCHAR(11) CHARACTER SET LATIN NOT CASESPECIFIC,
      department_name VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_id VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      group_nm VARCHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      category_id VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      category_nm VARCHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      cpc VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      div_promo_grp_cd INTEGER,
      rank_by_rog_and_cpc VARCHAR(18) CHARACTER SET LATIN NOT CASESPECIFIC,
      avg_net_sales_13_wk DECIMAL(18,2),
      avg_item_qty_13_wk DECIMAL(18,3),
      num_stores_selling INTEGER,
      num_stores_in_rog INTEGER)
PRIMARY INDEX ( rog_cd ,upc_id );
.IF ERRORLEVEL <> 0 THEN .QUIT;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
(       division_id
,       rpt_group
,       rog_cd
,       upc_id
,       ROG_UPC
,       upc_dsc
,       department_id
,       department_name
,       group_id
,       group_nm
,       category_id
,       category_nm
,       cpc
,       div_promo_grp_cd
,       rank_by_rog_and_cpc
,       avg_net_sales_13_wk
,       avg_item_qty_13_wk
,       num_stores_selling
,       num_stores_in_rog
)
SELECT  dt.division_id
,		dt.rpt_group
,       dt.rog_cd
,       dt.upc_id
,       dt.rog_upc
,       dt.upc_dsc
,       dt.department_id
,       dt.department_name
,       dt.group_id
,       dt.group_nm
,       dt.category_id
,       dt.category_nm
,       dt.cpc
,       dt.div_promo_grp_cd
,       dt.rank_by_rog_and_cpc
,       dt.avg_net_sales_13_wk
,       dt.avg_item_qty_13_wk
,       dt.num_stores_selling
,       tb1.total_stores AS num_stores_in_rog
FROM    (
        SELECT  str.division_id
        ,       CASE
                    WHEN str.store_id = 199 THEN 'S30_02'
                    WHEN str.store_id IN (339, 1509, 1708) THEN 'S30_03'
                    WHEN str.store_id IN (1911,1912,2089,2105,2203,2210,2212,2214,2215,2216,2217,2224,2225,2226,2228,2229,2231,2233,2235,2412,2739,2803,2813,3005,3237) THEN 'S29_02'
                END AS rpt_group
        ,       str.rog_cd
        ,       upc.upc_id
        ,       str.rog_cd || '-' || TRIM(cast(upc.upc_id AS BIGINT)) AS rog_upc
        ,       upc.upc_dsc
        ,       TRIM(upc.department_id) AS department_id
        ,       TRIM(upc.department_nm) AS department_name
        ,       TRIM(upc.group_id)      AS group_id
        ,       TRIM(upc.group_nm)      AS group_nm
        ,       TRIM(upc.category_id)   AS category_id
        ,       TRIM(upc.category_nm)   AS category_nm
        ,       CASE WHEN upc.common_promo_cd <> 0
                    THEN upc.common_promo_cd ELSE ''
                END AS cpc
        ,       urx.div_promo_grp_cd
        ,       CASE
                    WHEN upc.common_promo_cd <> 0
                        THEN str.rog_cd || '_' || RIGHT('00' || TRIM(ROW_NUMBER() OVER(
                            PARTITION BY str.rog_cd,upc.common_promo_cd
                            ORDER BY avg_net_sales_13_wk DESC)),3)
                    ELSE ''
                END AS rank_by_rog_and_cpc
        ,       SUM(agp.sum_net_amt) /13    AS avg_net_sales_13_wk
        ,       SUM(agp.sum_item_qty)/13    AS avg_item_qty_13_wk
        ,       COUNT (UNIQUE str.store_id) AS num_stores_selling
        FROM    ${DWH_DSS_DB}.store_upc_agp agp
        INNER   JOIN ${DWH_DSS_DB}.lu_store str
        ON      str.store_id = agp.store_id
        AND     str.corporation_id = 1
        LEFT    OUTER JOIN (
                SELECT  rog_id
                ,       upc_Id
                ,       div_promo_grp_cd
                FROM    (
                        SELECT  rog_id
                        ,       upc_id
                        ,       div_promo_grp_cd
                        ,       COUNT(1) AS cnt
                        FROM    ${DWH_DSS_DB}.cic_upc_rog
                        WHERE   last_eff_dt = DATE '9999-12-31'
                        AND     div_promo_grp_cd > 0
                        GROUP   BY 1,2,3
                        ) dt
                QUALIFY ROW_NUMBER() OVER (PARTITION BY rog_id, upc_id ORDER BY cnt DESC, div_promo_grp_cd DESC) = 1
                ) urx
        ON      urx.rog_id = str.rog_id
        AND     urx.upc_id = agp.upc_id
        INNER   JOIN ${DWH_DSS_DB}.lu_upc upc
        ON      upc.upc_id = agp.upc_id
        AND     upc.corporation_id = 1
        WHERE   upc.category_id NOT IN (1350, 1390,1995,3290,7301,7305,7306,7310,7315,
                7320,7325,7330,7335,7340,7345,7401,7402,7405,
                7410,7415,7416,7420,7425,7430,7445,7450,7470,
                7480,7485,7490)
        AND     agp.day_dt BETWEEN CURRENT_DATE - 93 AND CURRENT_DATE - 3
        AND     NOT (agp.sum_net_amt <= 0 and agp.sum_item_qty <= 0)
        AND     str.closed_dt > CURRENT_DATE
        AND     upc.group_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,36,37,38,39,40,42,43,44,45,46,47,48,73,74,96,97)
        AND     str.store_id IN (199 -- S30_02
                , 339, 1509, 1708 -- 'S30_03'
                , 1911,1912,2089,2105,2203,2210,2212,2214,2215,2216,2217,2224,2225,2226,2228,2229,2231,2233,2235,2412,2739,2803,2813,3005,3237 -- 'S29_02'
                )
        GROUP   BY str.division_id
        ,		rpt_group
        ,       str.rog_cd
        ,       upc.upc_id
        ,       upc.upc_dsc
        ,       upc.department_id
        ,       upc.department_nm
        ,       upc.group_id
        ,       upc.group_nm
        ,       upc.category_id
        ,       upc.category_nm
        ,       upc.common_promo_cd
        ,       urx.div_promo_grp_cd
        ) dt
INNER   JOIN    (
        SELECT  str.rog_cd
        ,       CASE
                    WHEN str.store_id = 199 THEN 'S30_02'
                    WHEN str.store_id IN (339, 1509, 1708) THEN 'S30_03'
                    WHEN str.store_id IN (1911,1912,2089,2105,2203,2210,2212,2214,2215,2216,2217,2224,2225,2226,2228,2229,2231,2233,2235,2412,2739,2803,2813,3005,3237) THEN 'S29_02'
                END AS rpt_group
        ,       COUNT(UNIQUE str.store_id) AS total_stores
        FROM    ${DWH_DSS_DB}.lu_store str
        WHERE   str.corporation_id = 1
        AND     str.closed_dt > CURRENT_DATE
        AND     str.store_id IN (199, 339, 1509, 1708  -- div 30 special reports
                ,1911,1912,2089,2105,2203,2210,2212,2214,2215,2216,2217,2224,2225,2226,2228,2229,2231,2233,2235,2412,2739,2803,2813,3005,3237 -- div 29 special reports
                )
        GROUP   BY str.rog_cd
        ,       rpt_group
        ) AS tb1
ON      dt.rog_cd = tb1.rog_cd
AND     dt.rpt_group = tb1.rpt_group;

.IF ERRORLEVEL <> 0 THEN .QUIT;

DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_final;
CREATE SET TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_final ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      division_id INTEGER,
      rpt_group VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      promo_no_allowance VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      allow_no_promo VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      missing_allowance VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      cost_change VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ad_plan CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      less_10_Promo VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      less_10_allowance VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      greater_100_pass_through VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      t_09_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      lead_item VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Dominant_Price_Area CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      t_OOB VARCHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      sskvi CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      group_cd SMALLINT,
      group_nm CHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      SMIC CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      SMIC_name CHAR(46) CHARACTER SET LATIN NOT CASESPECIFIC,
      rog VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,
      price_area_id INTEGER,
      PA_name CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      pricing_role CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      OOB_gap_id CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      DIV_PROMO_GRP_CD INTEGER,
      loc_common_retail_cd DECIMAL(5,0),
      vendor_name VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_nbr CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_sub_acct_nbr CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      cost_area_id SMALLINT,
      Manuf CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      upc_id DECIMAL(13,0),
      corp_item_cd DECIMAL(8,0),
      item_description VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      DST CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      FACILITY CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      dst_stat CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      rtl_stat CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      buyer_nm CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      vend_conv_fctr SMALLINT,
      t_pack_whse_qty DECIMAL(10,0),
      size_dsc CHAR(7) CHARACTER SET LATIN NOT CASESPECIFIC,
      Row_Offset CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      UPC_13_Wk_Avg_Sales DECIMAL(18,2),
      UPC_13_Wk_Avg_Qty DECIMAL(18,3),
      UPC_13_Wk_Avg_RTL DECIMAL(18,3),
      T_RANK_BY_ROG_AND_CPC INTEGER,
      pct_ACV_Stores DECIMAL(5,2),
      t_CPC_13_Wk_Avg_Sales DECIMAL(18,2),
      t_CPC_13_Wk_Avg_Qty DECIMAL(18,3),
      t_CPC_13_Wk_Avg_RTL DECIMAL(18,3),
      PND_Cost_Change_VND DECIMAL(15,4),
      PND_VEN_Date_Effective DATE FORMAT 'YY/MM/DD',
      New_Recc_Reg_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Vendor_Unit_Cost DECIMAL(10,3),
      Unit_Item_Billing_Cost DECIMAL(10,3),
      Prev_Retail_Price_Fctr DECIMAL(2,0),
      Previous_Retail_Price DECIMAL(7,2),
      Prev_Retail_Effective_Date DATE FORMAT 'YY/MM/DD',
      Pending_EDLP_Mult DECIMAL(2,0),
      Pending_EDLP_Retail DECIMAL(7,2),
      Pending_EDLP_Chg_Date DATE FORMAT 'YY/MM/DD',
      Pending_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Reg_Retail_Price_Fctr DECIMAL(2,0),
      Reg_Retail DECIMAL(10,2),
      t_price_Per DECIMAL(10,3),
      t_Unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      Reg_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      case_allow_count INTEGER,
      case_allow_amt DECIMAL(15,4),
      Case_Allow_amt_per_Unit DECIMAL(15,4),
      Case_Start_Date DATE FORMAT 'YY/MM/DD',
      Case_End_Date DATE FORMAT 'YY/MM/DD',
      S2S_Allow_count INTEGER,
      S2S_Allow_amt DECIMAL(15,4),
      S2S_Allow_amt_per_Unit DECIMAL(15,4),
      S2S_Start_Date DATE FORMAT 'YY/MM/DD',
      S2S_End_Date DATE FORMAT 'YY/MM/DD',
      Scan_Allow_count INTEGER,
      Scan_Allow_amt DECIMAL(15,4),
      Scan_Start_Date DATE FORMAT 'YY/MM/DD',
      Scan_End_Date DATE FORMAT 'YY/MM/DD',
      Redem_Scan_Allow_count INTEGER,
      Redem_Allow_amt DECIMAL(15,4),
      Redem_Start_Date DATE FORMAT 'YY/MM/DD',
      Redem_End_Date DATE FORMAT 'YY/MM/DD',
      Total_Allow_Unit DECIMAL(15,4),
      t_Allowance_pctg DECIMAL(5,2),
      Net_Cost_with_Allow CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Promo_Multiple DECIMAL(3,0),
      Promo_Price DECIMAL(10,4),
      Coupon_Method CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      Min_Purch DECIMAL(2,0),
      Limit_Per_Txn SMALLINT,
      Promo_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Net_Promo_Price DECIMAL(10,2),
      Price_Per DECIMAL(10,3),
      t2_Unit VARCHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      t_markdown_pctg DECIMAL(15,2),
      Mark_down DECIMAL(10,2),
      Promo_Start DATE FORMAT 'YY/MM/DD',
      Promo_End DATE FORMAT 'YY/MM/DD',
      pass_through DECIMAL(5,2),
      NEW_Multiple CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_EDLP_GP_pctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_Multiple CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_Retail CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Promo_GPpctg CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NEW_Passthrough CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      compet_code VARCHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC,
      price_chk_date DATE FORMAT 'YY/MM/DD',
      comp_reg_mult DECIMAL(2,0),
      com_reg_price DECIMAL(7,2),
      REG_CPI CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      COMP_AD_MULT DECIMAL(2,0),
      COMP_AD_PRICE DECIMAL(7,2),
      Comments CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Modified_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ROG_and_CIG VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Allowance_Counts CHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      Report_Date DATE FORMAT 'YY/MM/DD')
PRIMARY INDEX ( rog ,upc_id ,FACILITY );
.IF ERRORLEVEL <> 0 THEN .QUIT;

INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_final
(       division_id
,       rpt_group
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       t_OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       DIV_PROMO_GRP_CD
,       loc_common_retail_cd
,       vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       Manuf
,       upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       t_pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       T_RANK_BY_ROG_AND_CPC
,       pct_ACV_Stores
,       t_CPC_13_Wk_Avg_Sales
,       t_CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
,       Unit_Item_Billing_Cost
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per
,       t_Unit
,       Reg_GP_pctg
,       case_allow_count
,       case_allow_amt
,       Case_Allow_amt_per_Unit
,       Case_Start_Date
,       Case_End_Date
,       S2S_Allow_count
,       S2S_Allow_amt
,       S2S_Allow_amt_per_Unit
,       S2S_Start_Date
,       S2S_End_Date
,       Scan_Allow_count
,       Scan_Allow_amt
,       Scan_Start_Date
,       Scan_End_Date
,       Redem_Scan_Allow_count
,       Redem_Allow_amt
,       Redem_Start_Date
,       Redem_End_Date
,       Total_Allow_Unit
,       t_Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       t_markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       pass_through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code
,       price_chk_date
,       comp_reg_mult
,       com_reg_price
,       REG_CPI
,       COMP_AD_MULT
,       COMP_AD_PRICE
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Allowance_Counts
,       Report_Date
)
SELECT  dsd.division_id
,       CASE
            WHEN dsd.division_id = 34 AND dsd.price_area_id IN (87, 3)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^ACME PAs 87 & 03'
            WHEN dsd.division_id = 33 AND dsd.price_area_id IN (4, 10, 23, 61)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^ASHA PAs 04,23,61 & 10'
            WHEN dsd.division_id = 32 AND dsd.price_area_id IN (1)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^AJWL PA 01'
            WHEN dsd.division_id = 30 AND dsd.price_area_id IN (2,4,6,8)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^AIMT PAs 02, 04, 06, 08 ONLY'
            WHEN dsd.division_id = 5 AND dsd.price_area_id IN (2,41, 51, 71)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^SDEN PAs 02, 41, 51, 71'
            WHEN dsd.division_id = 25 AND dsd.price_area_id IN (11,17,18,47)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^SHAW 47, SNCA 11, 17, 18'
            WHEN dsd.division_id = 29 AND dsd.price_area_id IN (1,3)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^VSOC 01 & 03, ASOCs 01 & 03'
            WHEN dsd.division_id = 19 AND dsd.price_area_id IN (1,69,3,75)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^SPRT 01, 69, 75 & APOR 03'
            WHEN dsd.division_id = 17 AND dsd.price_area_id IN (1,31,41,71)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^SPHO 01, APHO 31 & 41, ALAS 71'
            WHEN dsd.division_id = 35 AND dsd.price_area_id IN (40, 49, 53) AND dsd.group_cd > 1
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^[ALL GROUPS] - SEAS PAs 40, 49, 53'
            WHEN dsd.division_id = 27 AND dsd.group_cd IN (96,97) AND dsd.price_area_id IN (8,33,47,60)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^Beth & Randy' || '^SSEA 33 & 47, SSPK 60, & SACG 08'
            WHEN dsd.division_id = 27 AND dsd.group_cd IN (96,97) AND dsd.price_area_id NOT IN (8,33,47,60)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^Beth & Randy'
            WHEN dsd.division_id = 27 AND dsd.price_area_id IN (8,33,47,60)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^SSEA 33 & 47, SSPK 60, & SACG 08'
            WHEN dsd.division_id = 20 AND dsd.price_area_id IN (1,4,33,83)
                THEN (dsd.group_cd (FORMAT '99') (CHAR(2))) || '^RDAL 83, ADAL 04, RHOU 33, AHOU 01'
            ELSE    (dsd.group_cd (FORMAT '99') (CHAR(2)))
        END AS rpt_group
,       CASE WHEN COALESCE(dsd.Net_Promo_Price, 0) > 0 AND Total_Allow_Unit = 0 THEN 'Y' ELSE ' ' END AS promo_no_allowance
,       CASE WHEN COALESCE(dsd.S2S_Allow_amt, 0) + COALESCE(dsd.Scan_Allow_amt, 0) > 0 AND COALESCE(dsd.Net_Promo_Price, 0) = 0 THEN 'Y' ELSE ' ' END AS allow_no_promo
,       CASE WHEN malw.rog_and_cig IS NULL THEN ' ' ELSE 'Y' END AS missing_allowance
,       CASE WHEN dsd.PND_Cost_Change_VND IS NULL THEN ' ' ELSE 'Y' END AS cost_change
,       dsd.ad_plan
,       CASE WHEN t_markdown_pctg > 0 AND t_markdown_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_Promo  -- dsd.less_10_Promo
,       CASE WHEN t_Allowance_pctg > 0 AND t_Allowance_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_allowance  -- dsd.less_10_allowance
,       CASE WHEN Pass_Through > 1 THEN 'Y' ELSE ' ' END AS greater_100_pass_through  --dsd.greater_100_pass_through
,       dsd.t_09_Retail
,       CASE WHEN litm.division_id IS NULL THEN ' ' ELSE 'Y' END AS lead_item -- dsd.lead_item
,       dsd.Dominant_Price_Area
,       CASE WHEN dsd.manuf IN ('21130', '79893', '58200', '11535', '41303', '41130') THEN 'Y' ELSE ' ' END AS t_OOB  -- dsd.OOB
,       dsd.sskvi
,       dsd.group_cd
,       dsd.group_nm
,       dsd.SMIC
,       dsd.SMIC_name
,       CASE
            WHEN dsd.rog_cd = 'SEAS' THEN '1-SEAS'
            WHEN dsd.rog_cd = 'SEAG' THEN '2-SEAG'
            WHEN dsd.rog_cd = 'ACME' THEN '1-ACME'
            WHEN dsd.rog_cd = 'SDEN' THEN '1-SDEN'
            WHEN dsd.rog_cd = 'ADEN' THEN '2-ADEN'
            WHEN dsd.rog_cd = 'AIMT' THEN '1-AIMT'
            WHEN dsd.rog_cd = 'AJWL' THEN '1-AJWL'
            WHEN dsd.rog_cd = 'SNCA' THEN '1-SNCA'
            WHEN dsd.rog_cd = 'SHAW' THEN '2-SHAW'
            WHEN dsd.rog_cd = 'SPRT' THEN '1-SPRT'
            WHEN dsd.rog_cd = 'APOR' THEN '2-APOR'
            WHEN dsd.rog_cd = 'SSEA' THEN '1-SSEA'
            WHEN dsd.rog_cd = 'SSPK' THEN '2-SSPK'
            WHEN dsd.rog_cd = 'SACG' THEN '3-SACG'
            WHEN dsd.rog_cd = 'ASHA' THEN '1-ASHA'
            WHEN dsd.rog_cd = 'AVMT' THEN '2-AVMT'
            WHEN dsd.rog_cd = 'VSOC' THEN '1-VSOC'
            WHEN dsd.rog_cd = 'ASOC' THEN '2-ASOC'
            WHEN dsd.rog_cd = 'PSOC' THEN '3-PSOC'
            WHEN dsd.rog_cd = 'RDAL' THEN '1-RDAL'
            WHEN dsd.rog_cd = 'ADAL' THEN '2-ADAL'
            WHEN dsd.rog_cd = 'RHOU' THEN '3-RHOU'
            WHEN dsd.rog_cd = 'AHOU' THEN '4-AHOU'
            WHEN dsd.rog_cd = 'SPHO' THEN '1-SPHO'
            WHEN dsd.rog_cd = 'APHO' THEN '2-APHO'
            WHEN dsd.rog_cd = 'ALAS' THEN '3-ALAS'
            WHEN dsd.rog_cd = 'VLAS' THEN '4-VLAS'
            WHEN dsd.rog_cd = 'SPHX' THEN '5-SPHX'
            WHEN dsd.rog_cd = 'TEST' THEN '6-TEST'
            ELSE dsd.rog_cd
        END (VARCHAR(10)) AS rog
,       dsd.price_area_id               -- COLUMN S
,       dsd.PA_name                     -- COLUMN T
,       dsd.pricing_role
,       dsd.OOB_gap_id
,       dsd.DIV_PROMO_GRP_CD            -- COLUMN W CIG
,       dsd.loc_common_retail_cd        -- COLUMN X
,       dsd.vendor_name
,       dsd.vend_nbr
,       dsd.vend_sub_acct_nbr
,       dsd.cost_area_id
,       dsd.Manuf
,       dsd.upc_id
,       dsd.corp_item_cd
,       dsd.item_description
,       dsd.DST
,       dsd.FACILITY
,       dsd.dst_stat
,       dsd.rtl_stat
,       dsd.buyer_nm
,       dsd.vend_conv_fctr
,       dsd.t_pack_whse_qty
,       dsd.size_dsc
,       dsd.Row_Offset
,       sua.AVG_NET_SALES_13_WK     AS UPC_13_Wk_Avg_Sales           -- COLUMN: AP
,       sua.AVG_ITEM_QTY_13_WK      AS UPC_13_Wk_Avg_Qty
,       sua.AVG_NET_SALES_13_WK / NULLIFZERO(sua.AVG_ITEM_QTY_13_WK) AS UPC_13_Wk_Avg_RTL  -- dsd.UPC_13_Wk_Avg_RTL
,       CASE WHEN litm.division_id IS NULL THEN NULL ELSE 1 END (INTEGER) AS T_RANK_BY_ROG_AND_CPC
,       (sua.NUM_STORES_SELLING * 1.00) / (sua.NUM_STORES_IN_ROG * 1.00) * 100 (DECIMAL(5,2)) AS pct_ACV_Stores
,       COALESCE(cig.cig_sum_AVG_NET_SALES_13_WK, upc.upc_sum_AVG_NET_SALES_13_WK) AS t_CPC_13_Wk_Avg_Sales -- dsd.CPC_13_Wk_Avg_Sales   -- COLUMN: AU
,       COALESCE(cig.cig_sum_AVG_ITEM_QTY_13_WK, upc.upc_sum_AVG_ITEM_QTY_13_WK) AS t_CPC_13_Wk_Avg_Qty  -- dsd.CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_Sales / NULLIFZERO(t_CPC_13_Wk_Avg_Qty)  AS t_CPC_13_Wk_Avg_RTL  --dsd.CPC_13_Wk_Avg_RTL
,       dsd.PND_Cost_Change_VND
,       dsd.PND_VEN_Date_Effective
,       dsd.New_Recc_Reg_Retail
,       dsd.Vendor_Unit_Cost
,       dsd.Unit_Item_Billing_Cost
,       dsd.Prev_Retail_Price_Fctr
,       dsd.Previous_Retail_Price
,       dsd.Prev_Retail_Effective_Date
,       dsd.Pending_EDLP_Mult
,       dsd.Pending_EDLP_Retail
,       dsd.Pending_EDLP_Chg_Date
,       dsd.Pending_GP_pctg
,       dsd.Reg_Retail_Price_Fctr
,       dsd.Reg_Retail
,       dsd.t_price_Per
,       dsd.t_Unit
,       dsd.Reg_GP_pctg
,       dsd.case_allow_count
,       dsd.case_allow_amt            AS case_allow_amt
,       dsd.Case_Allow_amt_per_Unit   AS Case_Allow_amt_per_Unit
,       dsd.Case_Start_Date
,       dsd.Case_End_Date
,       dsd.S2S_Allow_count
,       dsd.S2S_Allow_amt             AS S2S_Allow_amt
,       dsd.S2S_Allow_amt_per_Unit    AS S2S_Allow_amt_per_Unit
,       dsd.S2S_Start_Date
,       dsd.S2S_End_Date
,       dsd.Scan_Allow_count
,       dsd.Scan_Allow_amt            AS Scan_Allow_amt
,       dsd.Scan_Start_Date
,       dsd.Scan_End_Date
,       dsd.Redem_Scan_Allow_count
,       dsd.Redem_Allow_amt           AS Redem_Allow_amt
,       dsd.Redem_Start_Date
,       dsd.Redem_End_Date
,       COALESCE(dsd.Case_Allow_amt_per_Unit, 0) +  COALESCE(dsd.S2S_Allow_amt_per_Unit, 0) +  COALESCE(dsd.Scan_Allow_amt, 0) AS Total_Allow_Unit
,       (Total_Allow_Unit / NULLIFZERO(dsd.Vendor_Unit_Cost)) (DECIMAL(5,2)) AS t_Allowance_pctg -- dsd.Allowance_pctg    -- COLUMN CH
,       dsd.Net_Cost_with_Allow
,       dsd.Promo_Multiple
,       dsd.Promo_Price
,       dsd.Coupon_Method
,       dsd.Min_Purch
,       dsd.Limit_Per_Txn
,       dsd.Promo_GP_pctg
,       dsd.Net_Promo_Price
,       dsd.Price_Per
,       dsd.t2_Unit
,       dsd.Mark_down / NULLIFZERO(dsd.reg_retail) / NULLIFZERO(dsd.Reg_Retail_Price_Fctr) AS t_markdown_pctg -- dsd.Markdown_pctg
,       dsd.Mark_down
,       dsd.Promo_Start
,       dsd.Promo_End
,       (dsd.mark_down / NULLIFZERO(total_allow_unit)) (DECIMAL(5,2)) AS pass_through  -- dsd.Pass_Through  -- COLUMN CW
,       dsd.NEW_Multiple
,       dsd.NEW_Retail
,       dsd.NEW_EDLP_GP_pctg
,       dsd.NEW_Promo_Multiple
,       dsd.NEW_Promo_Retail
,       dsd.NEW_Promo_GPpctg
,       dsd.NEW_Passthrough
,       dsd.compet_code
,       dsd.price_chk_date
,       dsd.comp_reg_mult
,       dsd.com_reg_price
,       dsd.REG_CPI
,       dsd.COMP_AD_MULT
,       dsd.COMP_AD_PRICE
,       dsd.Comments
,       dsd.Modified_flag
,       dsd.ROG_and_CIG
,       dsd.Allowance_Counts
,       dsd.Report_Date
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6 dsd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_sua_sales sua
ON      sua.rog_cd = dsd.rog_cd
AND     sua.upc_id = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       upc_id
        ,       SUM(AVG_NET_SALES_13_WK) AS upc_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS upc_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales
        GROUP   BY 1,2
        ) upc
ON      upc.division_id = dsd.division_id
AND     upc.upc_id      = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       DIV_PROMO_GRP_CD
        ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS cig_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales
        GROUP   BY 1,2
        ) cig
ON      cig.division_id = dsd.division_id
AND     cig.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       div_promo_grp_cd
        ,       upc_id
        FROM    (
                SELECT  division_id
                ,       div_promo_grp_cd
                ,       upc_id
                ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
                WHERE   div_promo_grp_cd > 0
                FROM    ${DWH_STAGE_DB}.t_pe_sua_sales
                GROUP   BY 1,2,3
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY division_id, div_promo_grp_cd ORDER by cig_sum_AVG_NET_SALES_13_WK DESC, upc_id ASC) = 1
        ) litm
ON      litm.division_id = dsd.division_id
AND     litm.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
AND     litm.upc_id = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  rog_and_cig
        ,       COUNT(DISTINCT allowance_counts) AS cnt
        FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6
        GROUP   BY rog_and_cig
        HAVING  cnt > 1
        ) malw
ON      malw.rog_and_cig = dsd.rog_and_cig
WHERE   dsd.group_cd > 1
OR      (dsd.division_id = 35 AND group_cd = 1);
.IF ERRORLEVEL <> 0 THEN .QUIT;


-- Division 30, PA 19 special report
INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_final
(       division_id
,       rpt_group
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       t_OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       DIV_PROMO_GRP_CD
,       loc_common_retail_cd
,       vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       Manuf
,       upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       t_pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       T_RANK_BY_ROG_AND_CPC
,       pct_ACV_Stores
,       t_CPC_13_Wk_Avg_Sales
,       t_CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
,       Unit_Item_Billing_Cost
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per
,       t_Unit
,       Reg_GP_pctg
,       case_allow_count
,       case_allow_amt
,       Case_Allow_amt_per_Unit
,       Case_Start_Date
,       Case_End_Date
,       S2S_Allow_count
,       S2S_Allow_amt
,       S2S_Allow_amt_per_Unit
,       S2S_Start_Date
,       S2S_End_Date
,       Scan_Allow_count
,       Scan_Allow_amt
,       Scan_Start_Date
,       Scan_End_Date
,       Redem_Scan_Allow_count
,       Redem_Allow_amt
,       Redem_Start_Date
,       Redem_End_Date
,       Total_Allow_Unit
,       t_Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       t_markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       pass_through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code
,       price_chk_date
,       comp_reg_mult
,       com_reg_price
,       REG_CPI
,       COMP_AD_MULT
,       COMP_AD_PRICE
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Allowance_Counts
,       Report_Date
)
SELECT  dsd.division_id
,       'AIMT PA 19 ONLY' AS rpt_group
,       CASE WHEN COALESCE(dsd.Net_Promo_Price, 0) > 0 AND Total_Allow_Unit = 0 THEN 'Y' ELSE ' ' END AS promo_no_allowance
,       CASE WHEN COALESCE(dsd.S2S_Allow_amt, 0) + COALESCE(dsd.Scan_Allow_amt, 0) > 0 AND COALESCE(dsd.Net_Promo_Price, 0) = 0 THEN 'Y' ELSE ' ' END AS allow_no_promo
,       CASE WHEN malw.rog_and_cig IS NULL THEN ' ' ELSE 'Y' END AS missing_allowance
,       CASE WHEN dsd.PND_Cost_Change_VND IS NULL THEN ' ' ELSE 'Y' END AS cost_change
,       dsd.ad_plan
,       CASE WHEN t_markdown_pctg > 0 AND t_markdown_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_Promo  -- dsd.less_10_Promo
,       CASE WHEN t_Allowance_pctg > 0 AND t_Allowance_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_allowance  -- dsd.less_10_allowance
,       CASE WHEN Pass_Through > 1 THEN 'Y' ELSE ' ' END AS greater_100_pass_through  --dsd.greater_100_pass_through
,       dsd.t_09_Retail
,       CASE WHEN litm.division_id IS NULL THEN ' ' ELSE 'Y' END AS lead_item -- dsd.lead_item
,       dsd.Dominant_Price_Area
,       CASE WHEN dsd.manuf IN ('21130', '79893', '58200', '11535', '41303', '41130') THEN 'Y' ELSE ' ' END AS t_OOB  -- dsd.OOB
,       dsd.sskvi
,       dsd.group_cd
,       dsd.group_nm
,       dsd.SMIC
,       dsd.SMIC_name
,       CASE
            WHEN dsd.rog_cd = 'SEAS' THEN '1-SEAS'
            WHEN dsd.rog_cd = 'SEAG' THEN '2-SEAG'
            WHEN dsd.rog_cd = 'ACME' THEN '1-ACME'
            WHEN dsd.rog_cd = 'SDEN' THEN '1-SDEN'
            WHEN dsd.rog_cd = 'ADEN' THEN '2-ADEN'
            WHEN dsd.rog_cd = 'AIMT' THEN '1-AIMT'
            WHEN dsd.rog_cd = 'AJWL' THEN '1-AJWL'
            WHEN dsd.rog_cd = 'SNCA' THEN '1-SNCA'
            WHEN dsd.rog_cd = 'SHAW' THEN '2-SHAW'
            WHEN dsd.rog_cd = 'SPRT' THEN '1-SPRT'
            WHEN dsd.rog_cd = 'APOR' THEN '2-APOR'
            WHEN dsd.rog_cd = 'SSEA' THEN '1-SSEA'
            WHEN dsd.rog_cd = 'SSPK' THEN '2-SSPK'
            WHEN dsd.rog_cd = 'SACG' THEN '3-SACG'
            WHEN dsd.rog_cd = 'ASHA' THEN '1-ASHA'
            WHEN dsd.rog_cd = 'AVMT' THEN '2-AVMT'
            WHEN dsd.rog_cd = 'VSOC' THEN '1-VSOC'
            WHEN dsd.rog_cd = 'ASOC' THEN '2-ASOC'
            WHEN dsd.rog_cd = 'PSOC' THEN '3-PSOC'
            WHEN dsd.rog_cd = 'RDAL' THEN '1-RDAL'
            WHEN dsd.rog_cd = 'ADAL' THEN '2-ADAL'
            WHEN dsd.rog_cd = 'RHOU' THEN '3-RHOU'
            WHEN dsd.rog_cd = 'AHOU' THEN '4-AHOU'
            WHEN dsd.rog_cd = 'SPHO' THEN '1-SPHO'
            WHEN dsd.rog_cd = 'APHO' THEN '2-APHO'
            WHEN dsd.rog_cd = 'ALAS' THEN '3-ALAS'
            WHEN dsd.rog_cd = 'VLAS' THEN '4-VLAS'
            WHEN dsd.rog_cd = 'SPHX' THEN '5-SPHX'
            WHEN dsd.rog_cd = 'TEST' THEN '6-TEST'
            ELSE dsd.rog_cd
        END (VARCHAR(10)) AS rog
,       dsd.price_area_id               -- COLUMN S
,       dsd.PA_name                     -- COLUMN T
,       dsd.pricing_role
,       dsd.OOB_gap_id
,       dsd.DIV_PROMO_GRP_CD            -- COLUMN W CIG
,       dsd.loc_common_retail_cd        -- COLUMN X
,       dsd.vendor_name
,       dsd.vend_nbr
,       dsd.vend_sub_acct_nbr
,       dsd.cost_area_id
,       dsd.Manuf
,       dsd.upc_id
,       dsd.corp_item_cd
,       dsd.item_description
,       dsd.DST
,       dsd.FACILITY
,       dsd.dst_stat
,       dsd.rtl_stat
,       dsd.buyer_nm
,       dsd.vend_conv_fctr
,       dsd.t_pack_whse_qty
,       dsd.size_dsc
,       dsd.Row_Offset
,       sua.AVG_NET_SALES_13_WK     AS UPC_13_Wk_Avg_Sales           -- COLUMN: AP
,       sua.AVG_ITEM_QTY_13_WK      AS UPC_13_Wk_Avg_Qty
,       sua.AVG_NET_SALES_13_WK / NULLIFZERO(sua.AVG_ITEM_QTY_13_WK) AS UPC_13_Wk_Avg_RTL  -- dsd.UPC_13_Wk_Avg_RTL
,       CASE WHEN litm.division_id IS NULL THEN NULL ELSE 1 END (INTEGER) AS T_RANK_BY_ROG_AND_CPC
,       (sua.NUM_STORES_SELLING * 1.00) / (sua.NUM_STORES_IN_ROG * 1.00) * 100 (DECIMAL(5,2)) AS pct_ACV_Stores
,       COALESCE(cig.cig_sum_AVG_NET_SALES_13_WK, upc.upc_sum_AVG_NET_SALES_13_WK) AS t_CPC_13_Wk_Avg_Sales -- dsd.CPC_13_Wk_Avg_Sales   -- COLUMN: AU
,       COALESCE(cig.cig_sum_AVG_ITEM_QTY_13_WK, upc.upc_sum_AVG_ITEM_QTY_13_WK) AS t_CPC_13_Wk_Avg_Qty  -- dsd.CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_Sales / NULLIFZERO(t_CPC_13_Wk_Avg_Qty)  AS t_CPC_13_Wk_Avg_RTL  --dsd.CPC_13_Wk_Avg_RTL
,       dsd.PND_Cost_Change_VND
,       dsd.PND_VEN_Date_Effective
,       dsd.New_Recc_Reg_Retail
,       dsd.Vendor_Unit_Cost
,       dsd.Unit_Item_Billing_Cost
,       dsd.Prev_Retail_Price_Fctr
,       dsd.Previous_Retail_Price
,       dsd.Prev_Retail_Effective_Date
,       dsd.Pending_EDLP_Mult
,       dsd.Pending_EDLP_Retail
,       dsd.Pending_EDLP_Chg_Date
,       dsd.Pending_GP_pctg
,       dsd.Reg_Retail_Price_Fctr
,       dsd.Reg_Retail
,       dsd.t_price_Per
,       dsd.t_Unit
,       dsd.Reg_GP_pctg
,       dsd.case_allow_count
,       dsd.case_allow_amt            AS case_allow_amt
,       dsd.Case_Allow_amt_per_Unit   AS Case_Allow_amt_per_Unit
,       dsd.Case_Start_Date
,       dsd.Case_End_Date
,       dsd.S2S_Allow_count
,       dsd.S2S_Allow_amt             AS S2S_Allow_amt
,       dsd.S2S_Allow_amt_per_Unit    AS S2S_Allow_amt_per_Unit
,       dsd.S2S_Start_Date
,       dsd.S2S_End_Date
,       dsd.Scan_Allow_count
,       dsd.Scan_Allow_amt            AS Scan_Allow_amt
,       dsd.Scan_Start_Date
,       dsd.Scan_End_Date
,       dsd.Redem_Scan_Allow_count
,       dsd.Redem_Allow_amt           AS Redem_Allow_amt
,       dsd.Redem_Start_Date
,       dsd.Redem_End_Date
,       COALESCE(dsd.Case_Allow_amt_per_Unit, 0) +  COALESCE(dsd.S2S_Allow_amt_per_Unit, 0) +  COALESCE(dsd.Scan_Allow_amt, 0) AS Total_Allow_Unit
,       (Total_Allow_Unit / NULLIFZERO(dsd.Vendor_Unit_Cost)) (DECIMAL(5,2)) AS t_Allowance_pctg -- dsd.Allowance_pctg    -- COLUMN CH
,       dsd.Net_Cost_with_Allow
,       dsd.Promo_Multiple
,       dsd.Promo_Price
,       dsd.Coupon_Method
,       dsd.Min_Purch
,       dsd.Limit_Per_Txn
,       dsd.Promo_GP_pctg
,       dsd.Net_Promo_Price
,       dsd.Price_Per
,       dsd.t2_Unit
,       dsd.Mark_down / NULLIFZERO(dsd.reg_retail) / NULLIFZERO(dsd.Reg_Retail_Price_Fctr) AS t_markdown_pctg -- dsd.Markdown_pctg
,       dsd.Mark_down
,       dsd.Promo_Start
,       dsd.Promo_End
,       (dsd.mark_down / NULLIFZERO(total_allow_unit)) (DECIMAL(5,2)) AS pass_through  -- dsd.Pass_Through  -- COLUMN CW
,       dsd.NEW_Multiple
,       dsd.NEW_Retail
,       dsd.NEW_EDLP_GP_pctg
,       dsd.NEW_Promo_Multiple
,       dsd.NEW_Promo_Retail
,       dsd.NEW_Promo_GPpctg
,       dsd.NEW_Passthrough
,       dsd.compet_code
,       dsd.price_chk_date
,       dsd.comp_reg_mult
,       dsd.com_reg_price
,       dsd.REG_CPI
,       dsd.COMP_AD_MULT
,       dsd.COMP_AD_PRICE
,       dsd.Comments
,       dsd.Modified_flag
,       dsd.ROG_and_CIG
,       dsd.Allowance_Counts
,       dsd.Report_Date
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6 dsd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases sua
ON      sua.rog_cd = dsd.rog_cd
AND     sua.upc_id = dsd.upc_id
AND     sua.rpt_group = 'S30_02'
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       upc_id
        ,       SUM(AVG_NET_SALES_13_WK) AS upc_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS upc_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S30_02'
        GROUP   BY 1,2
        ) upc
ON      upc.division_id = dsd.division_id
AND     upc.upc_id      = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       DIV_PROMO_GRP_CD
        ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS cig_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S30_02'
        GROUP   BY 1,2
        ) cig
ON      cig.division_id = dsd.division_id
AND     cig.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       div_promo_grp_cd
        ,       upc_id
        FROM    (
                SELECT  division_id
                ,       div_promo_grp_cd
                ,       upc_id
                ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
                FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
                WHERE   div_promo_grp_cd > 0
                AND     rpt_group = 'S30_02'
                GROUP   BY 1,2,3
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY division_id, div_promo_grp_cd ORDER by cig_sum_AVG_NET_SALES_13_WK DESC, upc_id ASC) = 1
        ) litm
ON      litm.division_id = dsd.division_id
AND     litm.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
AND     litm.upc_id = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  rog_and_cig
        ,       COUNT(DISTINCT allowance_counts) AS cnt
        FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6
        GROUP   BY rog_and_cig
        HAVING  cnt > 1
        ) malw
ON      malw.rog_and_cig = dsd.rog_and_cig
WHERE   dsd.division_id = 30
and     dsd.price_area_id = 19
AND     dsd.group_cd > 1;
.IF ERRORLEVEL <> 0 THEN .QUIT;

-- Division 30, PA 13 & 14 special report
INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_final
(       division_id
,       rpt_group
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       t_OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       DIV_PROMO_GRP_CD
,       loc_common_retail_cd
,       vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       Manuf
,       upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       t_pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       T_RANK_BY_ROG_AND_CPC
,       pct_ACV_Stores
,       t_CPC_13_Wk_Avg_Sales
,       t_CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
,       Unit_Item_Billing_Cost
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per
,       t_Unit
,       Reg_GP_pctg
,       case_allow_count
,       case_allow_amt
,       Case_Allow_amt_per_Unit
,       Case_Start_Date
,       Case_End_Date
,       S2S_Allow_count
,       S2S_Allow_amt
,       S2S_Allow_amt_per_Unit
,       S2S_Start_Date
,       S2S_End_Date
,       Scan_Allow_count
,       Scan_Allow_amt
,       Scan_Start_Date
,       Scan_End_Date
,       Redem_Scan_Allow_count
,       Redem_Allow_amt
,       Redem_Start_Date
,       Redem_End_Date
,       Total_Allow_Unit
,       t_Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       t_markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       pass_through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code
,       price_chk_date
,       comp_reg_mult
,       com_reg_price
,       REG_CPI
,       COMP_AD_MULT
,       COMP_AD_PRICE
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Allowance_Counts
,       Report_Date
)
SELECT  dsd.division_id
,       'AIMT PAs 13 & 14 ONLY' AS rpt_group
,       CASE WHEN COALESCE(dsd.Net_Promo_Price, 0) > 0 AND Total_Allow_Unit = 0 THEN 'Y' ELSE ' ' END AS promo_no_allowance
,       CASE WHEN COALESCE(dsd.S2S_Allow_amt, 0) + COALESCE(dsd.Scan_Allow_amt, 0) > 0 AND COALESCE(dsd.Net_Promo_Price, 0) = 0 THEN 'Y' ELSE ' ' END AS allow_no_promo
,       CASE WHEN malw.rog_and_cig IS NULL THEN ' ' ELSE 'Y' END AS missing_allowance
,       CASE WHEN dsd.PND_Cost_Change_VND IS NULL THEN ' ' ELSE 'Y' END AS cost_change
,       dsd.ad_plan
,       CASE WHEN t_markdown_pctg > 0 AND t_markdown_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_Promo  -- dsd.less_10_Promo
,       CASE WHEN t_Allowance_pctg > 0 AND t_Allowance_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_allowance  -- dsd.less_10_allowance
,       CASE WHEN Pass_Through > 1 THEN 'Y' ELSE ' ' END AS greater_100_pass_through  --dsd.greater_100_pass_through
,       dsd.t_09_Retail
,       CASE WHEN litm.division_id IS NULL THEN ' ' ELSE 'Y' END AS lead_item -- dsd.lead_item
,       dsd.Dominant_Price_Area
,       CASE WHEN dsd.manuf IN ('21130', '79893', '58200', '11535', '41303', '41130') THEN 'Y' ELSE ' ' END AS t_OOB  -- dsd.OOB
,       dsd.sskvi
,       dsd.group_cd
,       dsd.group_nm
,       dsd.SMIC
,       dsd.SMIC_name
,       CASE
            WHEN dsd.rog_cd = 'SEAS' THEN '1-SEAS'
            WHEN dsd.rog_cd = 'SEAG' THEN '2-SEAG'
            WHEN dsd.rog_cd = 'ACME' THEN '1-ACME'
            WHEN dsd.rog_cd = 'SDEN' THEN '1-SDEN'
            WHEN dsd.rog_cd = 'ADEN' THEN '2-ADEN'
            WHEN dsd.rog_cd = 'AIMT' THEN '1-AIMT'
            WHEN dsd.rog_cd = 'AJWL' THEN '1-AJWL'
            WHEN dsd.rog_cd = 'SNCA' THEN '1-SNCA'
            WHEN dsd.rog_cd = 'SHAW' THEN '2-SHAW'
            WHEN dsd.rog_cd = 'SPRT' THEN '1-SPRT'
            WHEN dsd.rog_cd = 'APOR' THEN '2-APOR'
            WHEN dsd.rog_cd = 'SSEA' THEN '1-SSEA'
            WHEN dsd.rog_cd = 'SSPK' THEN '2-SSPK'
            WHEN dsd.rog_cd = 'SACG' THEN '3-SACG'
            WHEN dsd.rog_cd = 'ASHA' THEN '1-ASHA'
            WHEN dsd.rog_cd = 'AVMT' THEN '2-AVMT'
            WHEN dsd.rog_cd = 'VSOC' THEN '1-VSOC'
            WHEN dsd.rog_cd = 'ASOC' THEN '2-ASOC'
            WHEN dsd.rog_cd = 'PSOC' THEN '3-PSOC'
            WHEN dsd.rog_cd = 'RDAL' THEN '1-RDAL'
            WHEN dsd.rog_cd = 'ADAL' THEN '2-ADAL'
            WHEN dsd.rog_cd = 'RHOU' THEN '3-RHOU'
            WHEN dsd.rog_cd = 'AHOU' THEN '4-AHOU'
            WHEN dsd.rog_cd = 'SPHO' THEN '1-SPHO'
            WHEN dsd.rog_cd = 'APHO' THEN '2-APHO'
            WHEN dsd.rog_cd = 'ALAS' THEN '3-ALAS'
            WHEN dsd.rog_cd = 'VLAS' THEN '4-VLAS'
            WHEN dsd.rog_cd = 'SPHX' THEN '5-SPHX'
            WHEN dsd.rog_cd = 'TEST' THEN '6-TEST'
            ELSE dsd.rog_cd
        END (VARCHAR(10)) AS rog
,       dsd.price_area_id               -- COLUMN S
,       dsd.PA_name                     -- COLUMN T
,       dsd.pricing_role
,       dsd.OOB_gap_id
,       dsd.DIV_PROMO_GRP_CD            -- COLUMN W CIG
,       dsd.loc_common_retail_cd        -- COLUMN X
,       dsd.vendor_name
,       dsd.vend_nbr
,       dsd.vend_sub_acct_nbr
,       dsd.cost_area_id
,       dsd.Manuf
,       dsd.upc_id
,       dsd.corp_item_cd
,       dsd.item_description
,       dsd.DST
,       dsd.FACILITY
,       dsd.dst_stat
,       dsd.rtl_stat
,       dsd.buyer_nm
,       dsd.vend_conv_fctr
,       dsd.t_pack_whse_qty
,       dsd.size_dsc
,       dsd.Row_Offset
,       sua.AVG_NET_SALES_13_WK     AS UPC_13_Wk_Avg_Sales           -- COLUMN: AP
,       sua.AVG_ITEM_QTY_13_WK      AS UPC_13_Wk_Avg_Qty
,       sua.AVG_NET_SALES_13_WK / NULLIFZERO(sua.AVG_ITEM_QTY_13_WK) AS UPC_13_Wk_Avg_RTL  -- dsd.UPC_13_Wk_Avg_RTL
,       CASE WHEN litm.division_id IS NULL THEN NULL ELSE 1 END (INTEGER) AS T_RANK_BY_ROG_AND_CPC
,       (sua.NUM_STORES_SELLING * 1.00) / (sua.NUM_STORES_IN_ROG * 1.00) * 100 (DECIMAL(5,2)) AS pct_ACV_Stores
,       COALESCE(cig.cig_sum_AVG_NET_SALES_13_WK, upc.upc_sum_AVG_NET_SALES_13_WK) AS t_CPC_13_Wk_Avg_Sales -- dsd.CPC_13_Wk_Avg_Sales   -- COLUMN: AU
,       COALESCE(cig.cig_sum_AVG_ITEM_QTY_13_WK, upc.upc_sum_AVG_ITEM_QTY_13_WK) AS t_CPC_13_Wk_Avg_Qty  -- dsd.CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_Sales / NULLIFZERO(t_CPC_13_Wk_Avg_Qty)  AS t_CPC_13_Wk_Avg_RTL  --dsd.CPC_13_Wk_Avg_RTL
,       dsd.PND_Cost_Change_VND
,       dsd.PND_VEN_Date_Effective
,       dsd.New_Recc_Reg_Retail
,       dsd.Vendor_Unit_Cost
,       dsd.Unit_Item_Billing_Cost
,       dsd.Prev_Retail_Price_Fctr
,       dsd.Previous_Retail_Price
,       dsd.Prev_Retail_Effective_Date
,       dsd.Pending_EDLP_Mult
,       dsd.Pending_EDLP_Retail
,       dsd.Pending_EDLP_Chg_Date
,       dsd.Pending_GP_pctg
,       dsd.Reg_Retail_Price_Fctr
,       dsd.Reg_Retail
,       dsd.t_price_Per
,       dsd.t_Unit
,       dsd.Reg_GP_pctg
,       dsd.case_allow_count
,       dsd.case_allow_amt            AS case_allow_amt
,       dsd.Case_Allow_amt_per_Unit   AS Case_Allow_amt_per_Unit
,       dsd.Case_Start_Date
,       dsd.Case_End_Date
,       dsd.S2S_Allow_count
,       dsd.S2S_Allow_amt             AS S2S_Allow_amt
,       dsd.S2S_Allow_amt_per_Unit    AS S2S_Allow_amt_per_Unit
,       dsd.S2S_Start_Date
,       dsd.S2S_End_Date
,       dsd.Scan_Allow_count
,       dsd.Scan_Allow_amt            AS Scan_Allow_amt
,       dsd.Scan_Start_Date
,       dsd.Scan_End_Date
,       dsd.Redem_Scan_Allow_count
,       dsd.Redem_Allow_amt           AS Redem_Allow_amt
,       dsd.Redem_Start_Date
,       dsd.Redem_End_Date
,       COALESCE(dsd.Case_Allow_amt_per_Unit, 0) +  COALESCE(dsd.S2S_Allow_amt_per_Unit, 0) +  COALESCE(dsd.Scan_Allow_amt, 0) AS Total_Allow_Unit
,       (Total_Allow_Unit / NULLIFZERO(dsd.Vendor_Unit_Cost)) (DECIMAL(5,2)) AS t_Allowance_pctg -- dsd.Allowance_pctg    -- COLUMN CH
,       dsd.Net_Cost_with_Allow
,       dsd.Promo_Multiple
,       dsd.Promo_Price
,       dsd.Coupon_Method
,       dsd.Min_Purch
,       dsd.Limit_Per_Txn
,       dsd.Promo_GP_pctg
,       dsd.Net_Promo_Price
,       dsd.Price_Per
,       dsd.t2_Unit
,       dsd.Mark_down / NULLIFZERO(dsd.reg_retail) / NULLIFZERO(dsd.Reg_Retail_Price_Fctr) AS t_markdown_pctg -- dsd.Markdown_pctg
,       dsd.Mark_down
,       dsd.Promo_Start
,       dsd.Promo_End
,       (dsd.mark_down / NULLIFZERO(total_allow_unit)) (DECIMAL(5,2)) AS pass_through  -- dsd.Pass_Through  -- COLUMN CW
,       dsd.NEW_Multiple
,       dsd.NEW_Retail
,       dsd.NEW_EDLP_GP_pctg
,       dsd.NEW_Promo_Multiple
,       dsd.NEW_Promo_Retail
,       dsd.NEW_Promo_GPpctg
,       dsd.NEW_Passthrough
,       dsd.compet_code
,       dsd.price_chk_date
,       dsd.comp_reg_mult
,       dsd.com_reg_price
,       dsd.REG_CPI
,       dsd.COMP_AD_MULT
,       dsd.COMP_AD_PRICE
,       dsd.Comments
,       dsd.Modified_flag
,       dsd.ROG_and_CIG
,       dsd.Allowance_Counts
,       dsd.Report_Date
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6 dsd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases sua
ON      sua.rog_cd = dsd.rog_cd
AND     sua.upc_id = dsd.upc_id
AND     sua.rpt_group = 'S30_03'
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       upc_id
        ,       SUM(AVG_NET_SALES_13_WK) AS upc_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS upc_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S30_03'
        GROUP   BY 1,2
        ) upc
ON      upc.division_id = dsd.division_id
AND     upc.upc_id      = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       DIV_PROMO_GRP_CD
        ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS cig_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S30_03'
        GROUP   BY 1,2
        ) cig
ON      cig.division_id = dsd.division_id
AND     cig.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       div_promo_grp_cd
        ,       upc_id
        FROM    (
                SELECT  division_id
                ,       div_promo_grp_cd
                ,       upc_id
                ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
                FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
                WHERE   div_promo_grp_cd > 0
                AND     rpt_group = 'S30_03'
                GROUP   BY 1,2,3
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY division_id, div_promo_grp_cd ORDER by cig_sum_AVG_NET_SALES_13_WK DESC, upc_id ASC) = 1
        ) litm
ON      litm.division_id = dsd.division_id
AND     litm.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
AND     litm.upc_id = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  rog_and_cig
        ,       COUNT(DISTINCT allowance_counts) AS cnt
        FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6
        GROUP   BY rog_and_cig
        HAVING  cnt > 1
        ) malw
ON      malw.rog_and_cig = dsd.rog_and_cig
WHERE   dsd.division_id = 30
and     dsd.price_area_id IN (13, 14)
AND     dsd.group_cd > 1;
.IF ERRORLEVEL <> 0 THEN .QUIT;

-- Division 29, PA 88 & 89 special report
INSERT
INTO    ${DWH_STAGE_DB}.t_pe_dsd_whse_final
(       division_id
,       rpt_group
,       promo_no_allowance
,       allow_no_promo
,       missing_allowance
,       cost_change
,       ad_plan
,       less_10_Promo
,       less_10_allowance
,       greater_100_pass_through
,       t_09_Retail
,       lead_item
,       Dominant_Price_Area
,       t_OOB
,       sskvi
,       group_cd
,       group_nm
,       SMIC
,       SMIC_name
,       rog
,       price_area_id
,       PA_name
,       pricing_role
,       OOB_gap_id
,       DIV_PROMO_GRP_CD
,       loc_common_retail_cd
,       vendor_name
,       vend_nbr
,       vend_sub_acct_nbr
,       cost_area_id
,       Manuf
,       upc_id
,       corp_item_cd
,       item_description
,       DST
,       FACILITY
,       dst_stat
,       rtl_stat
,       buyer_nm
,       vend_conv_fctr
,       t_pack_whse_qty
,       size_dsc
,       Row_Offset
,       UPC_13_Wk_Avg_Sales
,       UPC_13_Wk_Avg_Qty
,       UPC_13_Wk_Avg_RTL
,       T_RANK_BY_ROG_AND_CPC
,       pct_ACV_Stores
,       t_CPC_13_Wk_Avg_Sales
,       t_CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_RTL
,       PND_Cost_Change_VND
,       PND_VEN_Date_Effective
,       New_Recc_Reg_Retail
,       Vendor_Unit_Cost
,       Unit_Item_Billing_Cost
,       Prev_Retail_Price_Fctr
,       Previous_Retail_Price
,       Prev_Retail_Effective_Date
,       Pending_EDLP_Mult
,       Pending_EDLP_Retail
,       Pending_EDLP_Chg_Date
,       Pending_GP_pctg
,       Reg_Retail_Price_Fctr
,       Reg_Retail
,       t_price_Per
,       t_Unit
,       Reg_GP_pctg
,       case_allow_count
,       case_allow_amt
,       Case_Allow_amt_per_Unit
,       Case_Start_Date
,       Case_End_Date
,       S2S_Allow_count
,       S2S_Allow_amt
,       S2S_Allow_amt_per_Unit
,       S2S_Start_Date
,       S2S_End_Date
,       Scan_Allow_count
,       Scan_Allow_amt
,       Scan_Start_Date
,       Scan_End_Date
,       Redem_Scan_Allow_count
,       Redem_Allow_amt
,       Redem_Start_Date
,       Redem_End_Date
,       Total_Allow_Unit
,       t_Allowance_pctg
,       Net_Cost_with_Allow
,       Promo_Multiple
,       Promo_Price
,       Coupon_Method
,       Min_Purch
,       Limit_Per_Txn
,       Promo_GP_pctg
,       Net_Promo_Price
,       Price_Per
,       t2_Unit
,       t_markdown_pctg
,       Mark_down
,       Promo_Start
,       Promo_End
,       pass_through
,       NEW_Multiple
,       NEW_Retail
,       NEW_EDLP_GP_pctg
,       NEW_Promo_Multiple
,       NEW_Promo_Retail
,       NEW_Promo_GPpctg
,       NEW_Passthrough
,       compet_code
,       price_chk_date
,       comp_reg_mult
,       com_reg_price
,       REG_CPI
,       COMP_AD_MULT
,       COMP_AD_PRICE
,       Comments
,       Modified_flag
,       ROG_and_CIG
,       Allowance_Counts
,       Report_Date
)
SELECT  dsd.division_id
,       'PSOC 88, 89' AS rpt_group
,       CASE WHEN COALESCE(dsd.Net_Promo_Price, 0) > 0 AND Total_Allow_Unit = 0 THEN 'Y' ELSE ' ' END AS promo_no_allowance
,       CASE WHEN COALESCE(dsd.S2S_Allow_amt, 0) + COALESCE(dsd.Scan_Allow_amt, 0) > 0 AND COALESCE(dsd.Net_Promo_Price, 0) = 0 THEN 'Y' ELSE ' ' END AS allow_no_promo
,       CASE WHEN malw.rog_and_cig IS NULL THEN ' ' ELSE 'Y' END AS missing_allowance
,       CASE WHEN dsd.PND_Cost_Change_VND IS NULL THEN ' ' ELSE 'Y' END AS cost_change
,       dsd.ad_plan
,       CASE WHEN t_markdown_pctg > 0 AND t_markdown_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_Promo  -- dsd.less_10_Promo
,       CASE WHEN t_Allowance_pctg > 0 AND t_Allowance_pctg < 0.1 THEN 'Y' ELSE ' ' END AS less_10_allowance  -- dsd.less_10_allowance
,       CASE WHEN Pass_Through > 1 THEN 'Y' ELSE ' ' END AS greater_100_pass_through  --dsd.greater_100_pass_through
,       dsd.t_09_Retail
,       CASE WHEN litm.division_id IS NULL THEN ' ' ELSE 'Y' END AS lead_item -- dsd.lead_item
,       dsd.Dominant_Price_Area
,       CASE WHEN dsd.manuf IN ('21130', '79893', '58200', '11535', '41303', '41130') THEN 'Y' ELSE ' ' END AS t_OOB  -- dsd.OOB
,       dsd.sskvi
,       dsd.group_cd
,       dsd.group_nm
,       dsd.SMIC
,       dsd.SMIC_name
,       CASE
            WHEN dsd.rog_cd = 'SEAS' THEN '1-SEAS'
            WHEN dsd.rog_cd = 'SEAG' THEN '2-SEAG'
            WHEN dsd.rog_cd = 'ACME' THEN '1-ACME'
            WHEN dsd.rog_cd = 'SDEN' THEN '1-SDEN'
            WHEN dsd.rog_cd = 'ADEN' THEN '2-ADEN'
            WHEN dsd.rog_cd = 'AIMT' THEN '1-AIMT'
            WHEN dsd.rog_cd = 'AJWL' THEN '1-AJWL'
            WHEN dsd.rog_cd = 'SNCA' THEN '1-SNCA'
            WHEN dsd.rog_cd = 'SHAW' THEN '2-SHAW'
            WHEN dsd.rog_cd = 'SPRT' THEN '1-SPRT'
            WHEN dsd.rog_cd = 'APOR' THEN '2-APOR'
            WHEN dsd.rog_cd = 'SSEA' THEN '1-SSEA'
            WHEN dsd.rog_cd = 'SSPK' THEN '2-SSPK'
            WHEN dsd.rog_cd = 'SACG' THEN '3-SACG'
            WHEN dsd.rog_cd = 'ASHA' THEN '1-ASHA'
            WHEN dsd.rog_cd = 'AVMT' THEN '2-AVMT'
            WHEN dsd.rog_cd = 'VSOC' THEN '1-VSOC'
            WHEN dsd.rog_cd = 'ASOC' THEN '2-ASOC'
            WHEN dsd.rog_cd = 'PSOC' THEN '3-PSOC'
            WHEN dsd.rog_cd = 'RDAL' THEN '1-RDAL'
            WHEN dsd.rog_cd = 'ADAL' THEN '2-ADAL'
            WHEN dsd.rog_cd = 'RHOU' THEN '3-RHOU'
            WHEN dsd.rog_cd = 'AHOU' THEN '4-AHOU'
            WHEN dsd.rog_cd = 'SPHO' THEN '1-SPHO'
            WHEN dsd.rog_cd = 'APHO' THEN '2-APHO'
            WHEN dsd.rog_cd = 'ALAS' THEN '3-ALAS'
            WHEN dsd.rog_cd = 'VLAS' THEN '4-VLAS'
            WHEN dsd.rog_cd = 'SPHX' THEN '5-SPHX'
            WHEN dsd.rog_cd = 'TEST' THEN '6-TEST'
            ELSE dsd.rog_cd
        END (VARCHAR(10)) AS rog
,       dsd.price_area_id               -- COLUMN S
,       dsd.PA_name                     -- COLUMN T
,       dsd.pricing_role
,       dsd.OOB_gap_id
,       dsd.DIV_PROMO_GRP_CD            -- COLUMN W CIG
,       dsd.loc_common_retail_cd        -- COLUMN X
,       dsd.vendor_name
,       dsd.vend_nbr
,       dsd.vend_sub_acct_nbr
,       dsd.cost_area_id
,       dsd.Manuf
,       dsd.upc_id
,       dsd.corp_item_cd
,       dsd.item_description
,       dsd.DST
,       dsd.FACILITY
,       dsd.dst_stat
,       dsd.rtl_stat
,       dsd.buyer_nm
,       dsd.vend_conv_fctr
,       dsd.t_pack_whse_qty
,       dsd.size_dsc
,       dsd.Row_Offset
,       sua.AVG_NET_SALES_13_WK     AS UPC_13_Wk_Avg_Sales           -- COLUMN: AP
,       sua.AVG_ITEM_QTY_13_WK      AS UPC_13_Wk_Avg_Qty
,       sua.AVG_NET_SALES_13_WK / NULLIFZERO(sua.AVG_ITEM_QTY_13_WK) AS UPC_13_Wk_Avg_RTL  -- dsd.UPC_13_Wk_Avg_RTL
,       CASE WHEN litm.division_id IS NULL THEN NULL ELSE 1 END (INTEGER) AS T_RANK_BY_ROG_AND_CPC
,       (sua.NUM_STORES_SELLING * 1.00) / (sua.NUM_STORES_IN_ROG * 1.00) * 100 (DECIMAL(5,2)) AS pct_ACV_Stores
,       COALESCE(cig.cig_sum_AVG_NET_SALES_13_WK, upc.upc_sum_AVG_NET_SALES_13_WK) AS t_CPC_13_Wk_Avg_Sales -- dsd.CPC_13_Wk_Avg_Sales   -- COLUMN: AU
,       COALESCE(cig.cig_sum_AVG_ITEM_QTY_13_WK, upc.upc_sum_AVG_ITEM_QTY_13_WK) AS t_CPC_13_Wk_Avg_Qty  -- dsd.CPC_13_Wk_Avg_Qty
,       t_CPC_13_Wk_Avg_Sales / NULLIFZERO(t_CPC_13_Wk_Avg_Qty)  AS t_CPC_13_Wk_Avg_RTL  --dsd.CPC_13_Wk_Avg_RTL
,       dsd.PND_Cost_Change_VND
,       dsd.PND_VEN_Date_Effective
,       dsd.New_Recc_Reg_Retail
,       dsd.Vendor_Unit_Cost
,       dsd.Unit_Item_Billing_Cost
,       dsd.Prev_Retail_Price_Fctr
,       dsd.Previous_Retail_Price
,       dsd.Prev_Retail_Effective_Date
,       dsd.Pending_EDLP_Mult
,       dsd.Pending_EDLP_Retail
,       dsd.Pending_EDLP_Chg_Date
,       dsd.Pending_GP_pctg
,       dsd.Reg_Retail_Price_Fctr
,       dsd.Reg_Retail
,       dsd.t_price_Per
,       dsd.t_Unit
,       dsd.Reg_GP_pctg
,       dsd.case_allow_count
,       dsd.case_allow_amt            AS case_allow_amt
,       dsd.Case_Allow_amt_per_Unit   AS Case_Allow_amt_per_Unit
,       dsd.Case_Start_Date
,       dsd.Case_End_Date
,       dsd.S2S_Allow_count
,       dsd.S2S_Allow_amt             AS S2S_Allow_amt
,       dsd.S2S_Allow_amt_per_Unit    AS S2S_Allow_amt_per_Unit
,       dsd.S2S_Start_Date
,       dsd.S2S_End_Date
,       dsd.Scan_Allow_count
,       dsd.Scan_Allow_amt            AS Scan_Allow_amt
,       dsd.Scan_Start_Date
,       dsd.Scan_End_Date
,       dsd.Redem_Scan_Allow_count
,       dsd.Redem_Allow_amt           AS Redem_Allow_amt
,       dsd.Redem_Start_Date
,       dsd.Redem_End_Date
,       COALESCE(dsd.Case_Allow_amt_per_Unit, 0) +  COALESCE(dsd.S2S_Allow_amt_per_Unit, 0) +  COALESCE(dsd.Scan_Allow_amt, 0) AS Total_Allow_Unit
,       (Total_Allow_Unit / NULLIFZERO(dsd.Vendor_Unit_Cost)) (DECIMAL(5,2)) AS t_Allowance_pctg -- dsd.Allowance_pctg    -- COLUMN CH
,       dsd.Net_Cost_with_Allow
,       dsd.Promo_Multiple
,       dsd.Promo_Price
,       dsd.Coupon_Method
,       dsd.Min_Purch
,       dsd.Limit_Per_Txn
,       dsd.Promo_GP_pctg
,       dsd.Net_Promo_Price
,       dsd.Price_Per
,       dsd.t2_Unit
,       dsd.Mark_down / NULLIFZERO(dsd.reg_retail) / NULLIFZERO(dsd.Reg_Retail_Price_Fctr) AS t_markdown_pctg -- dsd.Markdown_pctg
,       dsd.Mark_down
,       dsd.Promo_Start
,       dsd.Promo_End
,       (dsd.mark_down / NULLIFZERO(total_allow_unit)) (DECIMAL(5,2)) AS pass_through  -- dsd.Pass_Through  -- COLUMN CW
,       dsd.NEW_Multiple
,       dsd.NEW_Retail
,       dsd.NEW_EDLP_GP_pctg
,       dsd.NEW_Promo_Multiple
,       dsd.NEW_Promo_Retail
,       dsd.NEW_Promo_GPpctg
,       dsd.NEW_Passthrough
,       dsd.compet_code
,       dsd.price_chk_date
,       dsd.comp_reg_mult
,       dsd.com_reg_price
,       dsd.REG_CPI
,       dsd.COMP_AD_MULT
,       dsd.COMP_AD_PRICE
,       dsd.Comments
,       dsd.Modified_flag
,       dsd.ROG_and_CIG
,       dsd.Allowance_Counts
,       dsd.Report_Date
FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6 dsd
LEFT    OUTER JOIN ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases sua
ON      sua.rog_cd = dsd.rog_cd
AND     sua.upc_id = dsd.upc_id
AND     sua.rpt_group = 'S29_02'
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       upc_id
        ,       SUM(AVG_NET_SALES_13_WK) AS upc_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS upc_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S29_02'
        GROUP   BY 1,2
        ) upc
ON      upc.division_id = dsd.division_id
AND     upc.upc_id      = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       DIV_PROMO_GRP_CD
        ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
        ,       SUM(AVG_ITEM_QTY_13_WK)  AS cig_sum_AVG_ITEM_QTY_13_WK
        FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
        WHERE   rpt_group = 'S29_02'
        GROUP   BY 1,2
        ) cig
ON      cig.division_id = dsd.division_id
AND     cig.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
LEFT    OUTER JOIN (
        SELECT  division_id
        ,       div_promo_grp_cd
        ,       upc_id
        FROM    (
                SELECT  division_id
                ,       div_promo_grp_cd
                ,       upc_id
                ,       SUM(AVG_NET_SALES_13_WK) AS cig_sum_AVG_NET_SALES_13_WK
                FROM    ${DWH_STAGE_DB}.t_pe_sua_sales_sp_cases
                WHERE   div_promo_grp_cd > 0
                AND     rpt_group = 'S29_02'
                GROUP   BY 1,2,3
                ) dt
        QUALIFY ROW_NUMBER() OVER (PARTITION BY division_id, div_promo_grp_cd ORDER by cig_sum_AVG_NET_SALES_13_WK DESC, upc_id ASC) = 1
        ) litm
ON      litm.division_id = dsd.division_id
AND     litm.DIV_PROMO_GRP_CD = dsd.DIV_PROMO_GRP_CD
AND     litm.upc_id = dsd.upc_id
LEFT    OUTER JOIN (
        SELECT  rog_and_cig
        ,       COUNT(DISTINCT allowance_counts) AS cnt
        FROM    ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6
        GROUP   BY rog_and_cig
        HAVING  cnt > 1
        ) malw
ON      malw.rog_and_cig = dsd.rog_and_cig
WHERE   dsd.division_id = 29
and     dsd.price_area_id IN (88, 89)
AND     dsd.group_cd > 1;
.IF ERRORLEVEL <> 0 THEN .QUIT;

.LOGOFF;
.QUIT 0;
!

Res=$?
if [ ${Res} != 0 ]; then
    echo -e "ERROR:  Function PrepData failed"
    exit ${Res}
fi

echo -e "\n*** END : PrepData - `date` ***\n"
}

#------------------------------------------------------------------------------#
function PostCleanup
#------------------------------------------------------------------------------#
{
echo -e "\n*** START : PostCleanup - `date` ***\n"

bteq<<!
.SET SESSIONS 1;
.LOGON ${USERID}, ${PASSWORD};
.IF ERRORLEVEL <> 0 THEN .QUIT;

DROP TABLE ${DWH_STAGE_DB}.t_pe_alw_hdr_dtl_all;
DROP TABLE ${DWH_STAGE_DB}.t_pe_alw_final;
DROP TABLE ${DWH_STAGE_DB}.t_pe_cpn_adplan_all;
DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_reg_rtl;
DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_dsd_cost;
DROP TABLE ${DWH_STAGE_DB}.t_pe_pending_whse_cost;
DROP TABLE ${DWH_STAGE_DB}.t_pe_hist_reg_rtl;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_1;
DROP TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_1;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_item_attr_2;
DROP TABLE ${DWH_STAGE_DB}.t_pe_whse_item_attr_2;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_3;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_4;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_5;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_item_attr_6;
DROP TABLE ${DWH_STAGE_DB}.t_pe_sua_sales;
DROP TABLE ${DWH_STAGE_DB}.t_pe_dsd_whse_final;

.LOGOFF;
.QUIT 0;
!

echo -e "\n*** END : PostCleanup - `date` ***\n"
}

#------------------------------------------------------------------------------#
# MAIN
#------------------------------------------------------------------------------#
. ~/prod/bin/.dwh.cfg
. ~/prod/bin/.dwh_lib.ksh

ScriptName=`basename $0`
echo -e "\n*** START: ${ScriptName} - `date` ***"
set -x

export LD_LIBRARY_PATH=${TPT_LD_LIBRARY_PATH}

LotIdDate=$(LibLotToDate ${LOT_ID})

DWH_STAGE_DB=temp_ds
##DWH_DSS_DB=dw_dss

dateCtlFile=${DWH_OUT}/temp/perf_exec_process_date.txt
PrepRptDate
rptDate=`cat ${dateCtlFile}`

## 1.0
## Prepare table that will be exported
PrepData

## 1.1
###############################################################################
## START:  TO DO, replace values of variables as needed
## WARNING!!! DO NOT change variable names as these are used in multiple libraries

## Specify table to export
export TPT_EX_TD_SRC_DB_NAME=${DWH_STAGE_DB}
export TPT_EX_TD_SRC_TABLE_NAME="t_pe_dsd_whse_final"

## Specify extract file name
export TPT_EX_TD_TGT_EXPORT_FILE=${DWH_OUT}/temp/pricing_tool_data_${LotIdDate}.dat.gz
rm -f ${TPT_EX_TD_TGT_EXPORT_FILE}
## END:  TO DO
###############################################################################

## 1.2
## Generate Teradata TPT script
LibGenTptTdScript
echo "TPT script file:  ${TPT_EX_TD_SCRIPT_FILE}"

## 1.3
## Execute Teradata TPT script
##/opt/teradata/client/14.10/tbuild/bin/tbuild -f ${TPT_EX_TD_SCRIPT_FILE} -s 1 -j TPT_JOB_${TPT_EX_TD_SRC_TABLE_NAME}_$$
tbuild -f ${TPT_EX_TD_SCRIPT_FILE} -s 1 -j TPT_JOB_${TPT_EX_TD_SRC_TABLE_NAME}_$$

if [ ${Res} == 0 ]; then
    echo -e "INFO:  TPT job completed successfully"
elif [ ${Res} == 4 ];then
    echo -e "WARNING:  Warning encountered..."
else
    echo -e "ERROR:  Error encountered exporting ${srcDatabaseName}.${srcTableName}"
    rm -f ${TPT_EX_TD_SCRIPT_FILE}
    exit ${Res}
fi

rm -f ${TPT_EX_TD_SCRIPT_FILE}

## 1.5
## Specify target file name for Parquet schema file
export TPT_EX_TD_TGT_PARQUET_SCHEMA_FILE=${DWH_OUT}/temp/pricing_tool_data_${LotIdDate}.sch.gz

## 1.6
## Generate Parquet schema script
LibeGenTptParquetSchemaFile

## Final step, create tarball
cd ${DWH_OUT}/temp
tar -cvf APIC_pricing_tool_data_${LotIdDate}.tar pricing_tool_data_${LotIdDate}*.gz --remove-files
mv APIC_pricing_tool_data_${LotIdDate}.tar ${DWH_OUT}/.
##PostCleanup

set +x
echo -e "\n*** END : ${ScriptName} for LOT ${LOT_ID} - `date` ***"
