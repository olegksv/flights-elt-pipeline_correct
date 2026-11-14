from dagster import Definitions, ScheduleDefinition, define_asset_job, definitions

end_to_end_job = define_asset_job(
    name="end_to_end_flights_elt_job",
    selection="*",
    description="Сквозной ELT: загрузка курсов валют по API -> пересборка dbt моделей и тестов",
)

daily_schedule = ScheduleDefinition(
    job=end_to_end_job,
    cron_schedule="0 4 * * *",
)

@definitions
def schedule_defs():
    return Definitions(
        jobs=[end_to_end_job],
        schedules=[daily_schedule],
    )
