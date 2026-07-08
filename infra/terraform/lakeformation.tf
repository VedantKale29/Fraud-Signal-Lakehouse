# THE governance demo (SS6.2 gate): the analyst role can query the gold
# mart but wallet_id is EXCLUDED at column level. IAM gates services;
# Lake Formation gates data -- this is the proof.

resource "aws_lakeformation_permissions" "analyst_fact" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name         = aws_glue_catalog_database.gold.name
    name                  = "fact_transaction"
    wildcard              = true
    excluded_column_names = ["wallet_id", "counterparty_id"] # masked from analysts
  }
}

resource "aws_lakeformation_permissions" "analyst_dim" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT"]

  table_with_columns {
    database_name         = aws_glue_catalog_database.gold.name
    name                  = "dim_wallet"
    wildcard              = true
    excluded_column_names = ["wallet_id"]
  }
}
