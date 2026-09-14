import duckdb

SILVER_GLOB = "/data/silver/weather/**/*.parquet"

def main() -> None:
    result = duckdb.sql(
        f"""
        SELECT
            count(*) AS total_rows,
            count(*) FILTER (
                WHERE event_id IS NULL
            ) AS null_event_id,

            count(*) FILTER (
                WHERE city IS NULL
            ) AS null_city,

            count(*) FILTER (
                WHERE observed_at IS NULL
            ) AS null_observed_at,

            count(*) FILTER (
                WHERE temperature_c < -100
                   OR temperature_c > 70
            ) AS invalid_temperature

        FROM read_parquet(
            '{SILVER_GLOB}',
            hive_partitioning = true
        )
        """
    ).fetchone()

    (
        total_rows,
        null_event_id,
        null_city,
        null_observed_at,
        invalid_temperature,
    ) = result

    assert total_rows > 0, "Silver table is empty"
    assert null_event_id == 0, "Null event_id detected"
    assert null_city == 0, "Null city detected"
    assert null_observed_at == 0, "Null observed_at detected"
    assert invalid_temperature == 0, "Invalid temperature detected"

    print(
        "data_quality_passed=true "
        f"rows={total_rows}"
    )

if __name__ == "__main__":
    main()