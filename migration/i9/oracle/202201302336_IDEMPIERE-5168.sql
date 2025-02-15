SET SQLBLANKLINES ON
SET DEFINE OFF

-- IDEMPIERE-5168 column AD_AuthorizationAccount.AccessToken is short on some case
-- Jan 30, 2022, 11:18:41 PM CET
UPDATE AD_Column SET FieldLength=4000,Updated=TO_DATE('2022-01-30 23:18:41','YYYY-MM-DD HH24:MI:SS'),UpdatedBy=100 WHERE AD_Column_ID=8805
;

-- Jan 30, 2022, 11:18:43 PM CET
ALTER TABLE AD_ChangeLog MODIFY NewValue VARCHAR2(4000 CHAR) DEFAULT NULL 
;

-- Jan 30, 2022, 11:18:50 PM CET
UPDATE AD_Column SET FieldLength=4000,Updated=TO_DATE('2022-01-30 23:18:50','YYYY-MM-DD HH24:MI:SS'),UpdatedBy=100 WHERE AD_Column_ID=8815
;

-- Jan 30, 2022, 11:18:52 PM CET
ALTER TABLE AD_ChangeLog MODIFY OldValue VARCHAR2(4000 CHAR) DEFAULT NULL 
;

-- Jan 30, 2022, 11:32:57 PM CET
UPDATE AD_Column SET FieldLength=0, AD_Reference_ID=14,Updated=TO_DATE('2022-01-30 23:32:57','YYYY-MM-DD HH24:MI:SS'),UpdatedBy=100 WHERE AD_Column_ID=214402
;

-- Jan 30, 2022, 11:33:02 PM CET
ALTER TABLE AD_AuthorizationAccount ADD Tmp_AccessToken LONG DEFAULT NULL;
UPDATE AD_AuthorizationAccount SET Tmp_AccessToken = AccessToken;
ALTER TABLE AD_AuthorizationAccount DROP COLUMN AccessToken;
ALTER TABLE AD_AuthorizationAccount RENAME COLUMN Tmp_AccessToken TO AccessToken;

ALTER TABLE AD_AuthorizationAccount MODIFY AccessToken CLOB DEFAULT NULL 
;

UPDATE AD_Column
SET AD_Reference_ID=14, FieldLength=0
WHERE AD_Reference_ID=36
AND AD_Column_ID IN (
214590, /* AD_Package_Exp_Detail.ExecCode */
214591, /* AD_Package_Imp_Detail.ExecCode */
214592, /* AD_Package_Imp_Detail.Result */
1206, /* AD_WF_Node.Help */
2290, /* AD_WF_Node_Trl.Help */
238, /* AD_Workflow.Help */
316, /* AD_Workflow_Trl.Help */
51012, /* PA_DashboardContent.HTML */
210868, /* PA_DashboardContent_Trl.HTML */
5414, /* R_MailText.MailText */
14615 /* R_MailText_Trl.MailText */
)
;

SELECT register_migration_script('202201302336_IDEMPIERE-5168.sql') FROM dual
;

