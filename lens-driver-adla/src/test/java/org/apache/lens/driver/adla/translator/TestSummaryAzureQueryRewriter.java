/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.adla.translator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import static com.google.common.collect.Lists.newArrayList;

import org.apache.lens.api.LensConf;
import org.apache.lens.driver.adla.ADLADriver;
import org.apache.lens.driver.jdbc.ColumnarSQLRewriter;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.annotations.Test;


/**
 * Created on 15/1/18.
 */
public class TestSummaryAzureQueryRewriter {

  @Test
  public void testQueryRewrite() throws LensException {
    LensDriver driver = new ADLADriver();
    Configuration baseConf = new Configuration();
    driver.configure(baseConf, "adla", "adla1");
    assertNotNull(driver);
    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));
    SummaryAzureQueryRewriter summaryAzureQueryRewriter = new SummaryAzureQueryRewriter();
    String driverQuery = "SELECT (ua.adv_account_inc_id) as `adv_account_inc_id`, (ua.campaign_inc_id)"
      + " as `campaign_inc_id`, sum((ua.total_burn)) as `burn`, sum((ua.total_clicks)) as `total_clicks` "
      + "FROM yoda.daily_dfw1_dwh_pe_agg2_demand_fact_ua ua WHERE ((ua.event_time) in "
      + "('2018-01-01 00:00:00') and (ua.adv_account_inc_id) in (208261 , 180154)) GROUP BY "
      + "(ua.adv_account_inc_id), (ua.campaign_inc_id) HAVING (ua.total_clicks) > 0 AND (ua.total_burn)"
      + " > 0 ORDER BY burn asc LIMIT 10";
    QueryContext context = new QueryContext(driverQuery, "SA", new LensConf(), baseConf,
      newArrayList(driver));
    assertEquals("@input = EXTRACT accidental_clicks double, ad_content_id long, ad_format_id "
      + "long, ad_group_investment double, ad_inc_id string, adgroup_inc_id string, adgroup_objective_id"
      + " string, adv_account_inc_id long, adv_bill_event_type string, adv_business_segment_id long,"
      + " adv_origin_country_id long, agency_revenue double, billable_cpm_beacons double, billed_cpc_clicks"
      + " double, billed_installs double, billed_spend double, campaign_inc_id long, completed_views"
      + " double, cpc_impressions double, cpm_clicks double, cpm_impressions double, creative_charges"
      + " double, data_enrichment_cost double, event_time DateTime, first_quartiles double, fraud_clicks"
      + " double, fund_source_id long, inmobi_investment double, io_discount double, is_vta_enabled "
      + "string, matched_conversions double, media_plays double, nofund_revenue double, "
      + "non_billable_cpc_clicks double, non_billable_impressions double, other_valid_clicks double, "
      + "pricing_model_id long, process_time DateTime, pub_pay_event_type string, request_time DateTime, "
      + "resolved_slot_size string, second_quartiles double, served_impressions double, terminated_clicks"
      + " double, third_quartiles double, total_burn double, total_clicks double, total_installs double,"
      + " unbilled_installs double, volume_discount double, vqs_count double, vqs_sum double, "
      + "withhold_taxes double FROM \"/pipeSeparated/summary1/2018-01-14-{*}/part.csv\" USING "
      + "Extractors.Text(delimiter: '|', skipFirstNRows: 1);@filter = SELECT adv_account_inc_id AS "
      + "[adv_account_inc_id], campaign_inc_id AS [campaign_inc_id], total_burn AS [burn], total_clicks"
      + " AS [total_clicks] FROM @input  WHERE ((event_time) IN (DateTime.Parse(\"2018-01-01 00:00:00\")) AND "
      + "([adv_account_inc_id]) IN (208261 , 180154));@groupby = SELECT [adv_account_inc_id], "
      + "[campaign_inc_id], SUM([burn])  AS [burn], SUM([total_clicks])  AS [total_clicks] FROM @filter"
      + "  GROUP BY [adv_account_inc_id], [campaign_inc_id] HAVING ((([total_clicks]) > 0) AND (([burn])"
      + " > 0));@orderby = SELECT [adv_account_inc_id], [campaign_inc_id], [burn], [total_clicks] "
      + "FROM @groupby ORDER BY burn ASC OFFSET 0 ROWS  FETCH 10 ROWS; OUTPUT @orderby TO"
      + " \"/clusters/output/<<jobid>>.csv\" USING Outputters.Csv();",
      summaryAzureQueryRewriter.rewrite(context, driver)
    );
  }

  @Test
  public void regexParse() {
    SummaryAzureQueryRewriter summaryAzureQueryRewriter = new SummaryAzureQueryRewriter();
    assertEquals(summaryAzureQueryRewriter
      .replaceUSQLDate("BETWEEN \"2018-01-01 13:00:00\" AND \"2018-01-01 14:00:00\" ldjhf"),
      "BETWEEN DateTime.Parse(\"2018-01-01 13:00:00\") AND DateTime.Parse(\"2018-01-01 14:00:00\") ldjhf");
  }
}
