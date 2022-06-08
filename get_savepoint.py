import traceback
import os
import psycopg2
import pandas as pd

from utils import noblr_secrets, noblr_config


def main():
    cursor = conn.cursor()
    tables = ['cc_policy','cc_claim','cctl_validationlevel','cctl_typeofproperty','cctl_matterview','cctl_financialscalculationtype','cctl_exposuretexttype','cctl_questionsettype','cc_inboundrecords','cctl_zonetype','cctl_servicerequestkind','cc_domaingraphpurgelock','cctl_primaryphonetype','cctl_riagreement','cctl_outboundfileconfig','cctl_rulelookupcolumndef','cctl_claimindicator','cc_claimmetricrecalctime','cc_answer','cctl_claimantidtype','cctl_createdvia','cc_role_name_l10n','cctl_dashboardstattype','cctl_exposureabstractview','cctl_adsbusinessfunctiongroup','cctl_transactionstatus','cctl_agglimitcalccriteria','cctl_assignmentsearchtype','cctl_repairoptionchoice_ext','cctl_contactsearchresulttype','ccx_alert_acc','cctl_jurisdictionalformula','cc_exposuremetric','cctl_runtimepropertygroup','cctl_contacttagtype','cctl_alarmtype','cc_history','cctl_sortbyrange','cc_actpat_ssbj_l10n','cctl_instructiontype','cc_subrogation','cctl_dbupdatestatsrunnertype','cctl_activityclass','cctl_contactdestructstatuscat','cctl_jurisdictiontype','cc_inboundrecord','cc_evaluation','cctl_shapetype','cctl_exposuretype','cctl_claimmetriccategory','cctl_covtermpattern','cc_organization','cctl_aggregatelimittype','cctl_workflow','cc_taccountlineitem','cctl_benefitendreasontype','cctl_policyratingplan','cctl_notetopictype','cc_recoverytaccountlineitem','cctl_adsscoringsource','cc_transactionlineitem','cctl_hopcoverageform','cctl_paymenttype','cctl_riarrangementtype','cctl_largelossnotificationstat','cctl_settlemethod','cctl_isostatus','cctl_wcinjurytype','cc_vehicleowner','cctl_addresstype','cctl_assessmentstatus','cctl_riagreementcoveragetype','cctl_adsbusinessfunction','cctl_documentstatustype','cc_catastrophe','cctl_wcbenefitfactortype','cctl_grouptype','cctl_includedaystype','cc_questionset','cctl_vehicledirection','cctl_recurrenceday','cctl_gendertype','cctl_documenttype','cctl_validationissue','cctl_reviewservicetype','cctl_workloadclassification','cctl_adversepartydenialreason','cctl_venuetype','cctl_languagetype','cctl_contentlineitemschedule','cctl_dynamicactioncategory','cctl_vehcondtype','cctl_financialsearchfield','cctl_proximitysearchstatus','cctl_activitycalendarview','cctl_mattercourttype','cc_contactaddress','cctl_assessmentevent','cctl_basecriterionconfig','cc_actpat_desc_l10n','cctl_suittype','cc_matter','cctl_claimindicatorcriterion','cctl_userroleconstraint','cc_authoritylimit','cctl_datadistributiontype','cctl_claimsource','cctl_workflowhandler','cctl_subrostrategy','cc_citation','cctl_archivefinalstatus','cctl_claimclosedoutcometype','cctl_customconditionparamtype','cc_claimexception','cctl_offroadvehiclestyle','cctl_adstriggeringpointkey','cc_bizrule','cctl_ownertype','cctl_exposurestate','cc_deduction','cctl_classificationcondition','cc_sitrigger','cctl_instructioncategory','cctl_outboundrecordstatus','cctl_otherrisktype','cctl_resolutiontype','cc_inboundfileconfig','cctl_boattype','cctl_userexperiencetype','cctl_phonetype','cc_dashboardstats','cc_exposuretext','cctl_seccontributingfactors','cctl_parametertype','cctl_servicerequestoperation','cctl_covtermmodelval','cctl_reasonforuse','cctl_losscause','cc_activitypattern','cctl_paymentmethod','cctl_shnotificationtype','cctl_insurpaymethod_acc','cctl_compensabilitydecision','cctl_claimaccesstype','cctl_subroclosedoutcome','cctl_regiontype','cctl_activitytype','cctl_nicbreferral_ext','cctl_bulkinvoicestatus','cctl_mattercourtdistrict','cctl_wcbenefitfactorcategory','cctl_covtermmodelrest','cctl_coverageissueseverity','cctl_solrsearchentity','cctl_contactrole','cctl_workflowworkitem','cctl_resultingaction','cctl_losspartytype','cctl_riskunit','cctl_claimmetriclimit','cctl_freetextclaimsearchtype','cctl_rulebooleanoperator','cctl_bankaccount','cctl_batchprocesstype','cctl_phonecountrycode','cc_appcritclaimsegment','cctl_updateop','cctl_wcmedicaltreatmenttype','cctl_rescontributingfactors','cc_parameter','cctl_assessmentcontentaction','cctl_underwritinggrouptype','cctl_conditionfilter','cctl_recoverycategory','cctl_archivestate','cctl_lossarea','cc_subrogationsummary','cctl_witnessposition','cctl_documentsection','cctl_currency','cctl_adscontextdefinitionkey','cctl_vehiclestyle','cctl_icdbodysystem','cc_appcritlosstype','cctl_fulldenialreason','cctl_datefieldstosearchtype','cctl_synchstate','cctl_authoritylimittype','cc_catastrophezone','ccx_riskupdate_acc','cctl_historytype','cctl_entitysourcetype','cc_claimmetriclimit','cctl_mattertexttype','cctl_transactionlifecyclestate','cctl_servicerequestquotestatus','ccx_importfileorder','cctl_covtermmodeltype','cc_specsvcname_l10n','cctl_lookupcolumndatatype']
    for table in tables:
        cursor.execute(f'select max(gwcdac__timestampfolder) from claims_raw_new.{table};')
        max_time = cursor.fetchall()
        for row in max_time:
            print(row[0])
            with open(f'{HOME}/savepoint.txt','a+') as f:
                string = f'"{table}" : "{row[0]}",\n'
                f.write(string)


if __name__ == '__main__':

    # Instantiate config class, close the config file as soon as possible
    HOME = os.getcwd()
    configFileName = f'{HOME}/conf/config_dev.yml'
    configFile = open(configFileName, "r")
    config = noblr_config.NoblrConfig(configFile)
    configFile.close()

    # Create Noblr secrets connection
    secrets = noblr_secrets.NoblrSecrets(config.get_secret_name(), config.get_secret_region())
    secret = secrets.get_secret()

    conn = None
    try:
        conn = psycopg2.connect(
            host=config.get_postgres_jdbcUrl(),
            database=config.get_postgres_jdbcDatabase(),
            user=secret[config.get_postgres_user()],
            password=secret[config.get_postgres_pwd()])
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        error_msg = traceback.format_exc()
        print(error_msg)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
