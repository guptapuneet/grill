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

import static java.util.stream.Collectors.joining;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.cube.parse.CubeSemanticAnalyzer;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.jdbc.ColumnarSQLRewriter;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import lombok.extern.slf4j.Slf4j;


/**
 * Created on 15/1/18.
 */
@Slf4j
public class SummaryAzureQueryRewriter extends ColumnarSQLRewriter {

  public String rewrite(QueryContext queryContext, LensDriver driver) throws LensException {
    String usqlQuery = "@input = EXTRACT accidental_clicks double, ad_content_id long, ad_format_id "
      + "long, ad_group_investment double, ad_inc_id string, adgroup_inc_id string, adgroup_objective_id"
      + " string, adv_account_inc_id long, adv_bill_event_type string, adv_business_segment_id long,"
      + " adv_origin_country_id long, agency_revenue double, billable_cpm_beacons double,"
      + " billed_cpc_clicks double, billed_installs double, billed_spend double, campaign_inc_id"
      + " long, completed_views double, cpc_impressions double, cpm_clicks double, "
      + "cpm_impressions double, creative_charges double, data_enrichment_cost double, "
      + "event_time DateTime, first_quartiles double, fraud_clicks double, fund_source_id long,"
      + " inmobi_investment double, io_discount double, is_vta_enabled string, matched_conversions"
      + " double, media_plays double, nofund_revenue double, non_billable_cpc_clicks double,"
      + " non_billable_impressions double, other_valid_clicks double, pricing_model_id long,"
      + " process_time DateTime, pub_pay_event_type string, request_time DateTime, resolved_slot_size"
      + " string, second_quartiles double, served_impressions double, terminated_clicks double,"
      + " third_quartiles double, total_burn double, total_clicks double, total_installs double,"
      + " unbilled_installs double, volume_discount double, vqs_count double, vqs_sum double,"
      + " withhold_taxes double FROM \"/pipeSeparated/summary1/2018-01-14-{*}/part.csv\" USING"
      + " Extractors.Text(delimiter: '|', skipFirstNRows: 1);";

    return usqlQuery + rewrite(queryContext.getDriverQuery(driver), queryContext.getDriverConf(driver),
      queryContext.getHiveConf());
  }

  @Override
  public String rewrite(String query, Configuration conf, HiveConf metastoreConf) throws LensException {
    this.query = query;
    log.info("Input Query : {}", query);
    String reWritten = rewrite(HQLParser.parseHQL(query, metastoreConf), conf, metastoreConf, true);
    log.info("Rewritten Query : {}", reWritten);
    return reWritten;
  }

  public String rewrite(ASTNode currNode, Configuration conf, HiveConf metastoreConf, boolean resolveNativeTables)
    throws LensException {
    rewrittenQuery.setLength(0);
    reset();
    this.ast = currNode;

    ASTNode fromNode = HQLParser.findNodeByPath(currNode, TOK_FROM);
    if (fromNode != null) {
      if (fromNode.getChild(0).getType() == TOK_SUBQUERY) {
        log.warn("Subqueries in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Subqueries in from clause not supported by " + this + " Query : " + this.query);
      } else if (isOfTypeJoin(fromNode.getChild(0).getType())) {
        log.warn("Join in from clause not supported by {} Query : {}", this, this.query);
        throw new LensException("Join in from clause not supported by " + this + " Query : " + this.query);
      }
    }

    if (currNode.getToken().getType() == TOK_UNIONALL) {
      log.warn("Union queries are not supported by {} Query : {}", this, this.query);
      throw new LensException("Union queries are not supported by " + this + " Query : " + this.query);
    }

    String rewritternQueryText = rewrittenQuery.toString();
    if (currNode.getToken().getType() == TOK_QUERY) {
      try {
        buildUSQLQuery(conf, metastoreConf);
        rewritternQueryText = rewrittenQuery.toString();
        log.info("Rewritten query from build : " + rewritternQueryText);
      } catch (SemanticException e) {
        throw new LensException(e);
      }
    }
    return rewritternQueryText;
  }

  private boolean isOfTypeJoin(int type) {
    return (type == TOK_JOIN || type == TOK_LEFTOUTERJOIN || type == TOK_RIGHTOUTERJOIN
      || type == TOK_FULLOUTERJOIN || type == TOK_LEFTSEMIJOIN || type == TOK_UNIQUEJOIN);
  }


  public void buildUSQLQuery(Configuration conf, HiveConf hconf) throws SemanticException, LensException {
    analyzeInternal(conf, hconf);

    // Get the limit clause
    String limit = getLimitClause(ast);

    Map<String, String> aliasMap = getAliasMap(selectAST);
    List<String> groupByFields = getGroupByFields(groupByAST, aliasMap);
    Map<String, String> measureFields = newHashMap();
    for (String k : aliasMap.keySet()) {
      if (!groupByFields.contains(k)) {
        measureFields.put(k, aliasMap.get(k));
      }
    }
    String cubeName = getCubeNameAlias(fromAST);
    String havingExpr = getReplacedExpr(havingAST, measureFields, cubeName);
    String orderByExpr = getOrderByExpr(orderByAST, aliasMap, cubeName);
    String filterExpr = getReplacedExpr(whereAST, aliasMap, cubeName);
    // construct query with fact sub query
    constructQuery(aliasMap, filterExpr, groupByFields
      , havingExpr, orderByExpr, limit);

  }

  String replaceUSQLDate(String filterQuery) {
    String regex = "\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:00:00)\"";
    Matcher m = Pattern.compile(regex).matcher(filterQuery);
    while (m.find()) {
      String matchedStr = m.group();
      filterQuery = filterQuery.replaceAll(matchedStr, "DateTime.Parse("+matchedStr+")");
    }
    return filterQuery;
  }

  String getOrderByExpr(final ASTNode orderByAST, Map<String, String> aliasMap, String cubeName) {
    return getReplacedExpr(orderByAST, aliasMap, cubeName).replaceAll(" asc", " ASC")
      .replaceAll(" desc", " DESC");
  }

  String getCubeNameAlias(final ASTNode fromAST) {
    return fromAST.getChild(0).getChild(1).toString();
  }

  String getReplacedExpr(final ASTNode ast, Map<String, String> aliasMap, String cubeName) {
    if (ast == null) {
      return "";
    }
    String str = HQLParser.getString(ast, HQLParser.AppendMode.DEFAULT).replaceAll("`", "")
      .replaceAll("sum", "SUM").replaceAll(" and ", " AND ")
      .replaceAll(" or ", " OR ").replaceAll(" not ", " NOT ")
      .replaceAll(" in ", " IN ").replaceAll(" between ", " BETWEEN ")
      .replaceAll(" = ", " == ")
      .replaceAll("'", "\"");
    for (String k : aliasMap.keySet()) {
      str = str.replaceAll(cubeName + "." + k, aliasMap.get(k));
    }
    return replaceUSQLDate(str.replaceAll(cubeName + ".", ""));
  }


  List<String> getGroupByFields(final ASTNode groupAST, Map<String, String> aliasMap) {
    List<String> fieldList = newArrayList();
    if (groupAST != null) {
      for (Node node : groupAST.getChildren()) {
        fieldList.add(aliasMap.get(node.getChildren().get(1).toString()));
      }
    }
    return fieldList;
  }

  Map<String, String> getAliasMap(ASTNode selectAST) {
    Map<String, String> map = new LinkedHashMap<>();
    if (selectAST != null) {
      for (Node node : selectAST.getChildren()) {
        List<Node> childList = (List<Node>) node.getChildren();
        if (childList == null) {
          continue;
        }
        if (childList.size() != 2) {
          map.put(childList.get(0).getChildren().get(0).toString(),
            "[" + childList.get(0).getChildren().get(0).toString() + "]");
        } else if (childList.get(0).toString().startsWith("TOK_")) {
          map.put(childList.get(0).getChildren().get(1).getChildren().get(1).toString(),
            "[" + childList.get(1).toString() + "]");
        } else if (childList.get(0).toString().startsWith(".")) {
          map.put(childList.get(0).getChildren().get(1).toString(),
            "[" + childList.get(0).getChildren().get(1).toString() + "]");
        }
      }
    }
    return map;
  }

  public void analyzeInternal(Configuration conf, HiveConf hconf) throws SemanticException {
    CubeSemanticAnalyzer c1 = new CubeSemanticAnalyzer(conf, hconf);

    QB qb = new QB(null, null, false);

    if (!c1.doPhase1(ast, qb, c1.initPhase1Ctx(), null)) {
      return;
    }

    if (!qb.getSubqAliases().isEmpty()) {
      log.warn("Subqueries in from clause is not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Subqueries in from clause is not supported by " + this + " Query : " + this.query);
    }

    // Get clause name
    TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
    /* The clause name. */
    String clauseName = ks.first();

    if (qb.getParseInfo().getJoinExpr() != null) {
      log.warn("Join queries not supported by {} Query : {}", this, this.query);
      throw new SemanticException("Join queries not supported by " + this + " Query : " + this.query);
    }
    // Split query into trees
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }

    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }

    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }

    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }

    this.fromAST = HQLParser.findNodeByPath(ast, TOK_FROM);

  }

  private void constructQuery(
    Map<String, String> aliasMap, String filterExpr, List<String> groupByList, String havingExpr,
    String orderbyExpr, String limit) {

    log.info("In construct query ..");

    String variable;

    String aliasQuery = "@filter = SELECT " + aliasMap.keySet().stream().map(k -> k + " AS "
      + aliasMap.get(k)).collect(joining(", "))
      + " FROM @input " + (filterExpr.isEmpty() ? "" : " WHERE " + filterExpr) + ";";

    rewrittenQuery.append(aliasQuery);

    String groupByQuery = "@groupby = SELECT " + aliasMap.keySet().stream().map(f -> {
      if (groupByList.contains(aliasMap.get(f))) {
        return aliasMap.get(f);
      } else {
        return "SUM(" + aliasMap.get(f) + ")  AS " + aliasMap.get(f);
      }
    }).collect(joining(", ")) + " FROM @filter "
      + (groupByList.isEmpty() ? "" : " GROUP BY " + groupByList.stream().collect(joining(", ")))
      + (havingExpr.isEmpty() ? "" : " HAVING ") + havingExpr + ";";

    rewrittenQuery.append(groupByQuery);

    if (orderbyExpr.isEmpty()) {
      variable = "groupby";
    } else {
      variable = "orderby";
      String orderByQuery = "@orderby = SELECT "
        + aliasMap.keySet().stream().map(f -> aliasMap.get(f)).collect(joining(", "))
        + " FROM @groupby ORDER BY " + orderbyExpr + " OFFSET 0 ROWS "
        + (limit.isEmpty() ? "" : " FETCH " + limit + " ROWS") + ";";
      rewrittenQuery.append(orderByQuery);
    }

    rewrittenQuery.append(" OUTPUT @" + variable + " TO \"/clusters/output/<<jobid>>.csv\" USING"
      + " Outputters.Csv();");
  }

}
