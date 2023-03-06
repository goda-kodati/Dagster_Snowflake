from dagster_snowflake import snowflake_resource

my_snowflake_resources={
        "snowflake": snowflake_resource.configured(
            {
                "account": "kpa19933.us-east-1",  # required
                "user": "PBI_STAGE",  # required
                "password": {"env": "SNOWFLAKE_STAGE_PASSWORD"},  # required
                "database": "CAMP_STAGE",  # required
                "warehouse": "COMPUTE_WH",  # optional, defaults to default warehouse for the account
                "schema": "EDW",  # optional, defaults to PUBLIC
            }
        )
    }