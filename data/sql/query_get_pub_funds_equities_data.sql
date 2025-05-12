SELECT main.DATETIME AS DATETIME,
       MAIN.SYMBOL AS SYMBOL,
       MAIN.PRICE AS PRICE_FUND,
       MAIN."SECURITY NAME" AS "SECURITY NAME",
       MAIN.ISIN AS "ISIN",
       MAIN."SEDOL" AS "SEDOL",
       MAIN."FUND NAME" AS "FUND NAME",
       MAIN."REALISED P/L" as "REALISED P/L",
       MAIN."MARKET VALUE" as "MARKET VALUE",
       MAIN."QUANTITY" as "QUANTITY"
       FROM tbl_pub_fund_position_details main
                  INNER JOIN tbl_cfg_fund_config lkp on
    MAIN."FUND NAME"=lkp."FUND NAME"
        AND main."FINANCIAL TYPE"='Equities'
        AND DATE('{dte}') BETWEEN lkp."EFFECTIVE START DATE" AND lkp."EFFECTIVE END DATE"
        ORDER BY PRICE_FUND,DATETIME,SYMBOL;
